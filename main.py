import logging
import time
from http import HTTPStatus
from pathlib import Path

from dashscope import Application
from flasgger import Swagger
from flask import Flask, request, send_from_directory
from flask_cors import CORS

from data.chat_route_model import ChatRouteModel
from data.history_data import HistoryData
from data.merchant_registry import (
    ensure_registry_storage,
    enrich_route_result,
    list_merchants,
    merchant_registry_data,
    resolve_merchant,
    to_public_payload,
)
from data.result_model import ResultModel
from utils.cache_util import invalidate_merchant_rag_cache
from utils.config_util import get_config
from utils.context_builder import build_messages
from utils.dify_dataset_retriever import DifyDatasetRetriever
from utils.dify_knowledge_base_client import DifyAPIError, DifyKnowledgeBaseClient
from utils.logger_config import setup_logging
from utils.rag_asset_util import build_references, extract_images_from_docs


# 初始化日志和第三方库日志级别，避免默认输出过于冗长。
setup_logging(name="")
logging.getLogger("dashscope").setLevel(logging.INFO)
logging.getLogger("urllib3.connectionpool").setLevel(logging.INFO)

logger = logging.getLogger(__name__)
# 加载服务运行所需配置，例如模型、数据库和 Dify 路由参数。
configs = get_config("config.json")
if not configs:
    raise RuntimeError("config.json 加载失败，请检查 JSON 格式、文件编码或必要配置项。")

# 创建 Flask 应用并开启跨域与 Swagger 文档支持。
app = Flask(__name__)
CORS(app)
app.config["SWAGGER"] = {
    "title": "AI Bot API",
    "uiversion": 3,
    "openapi": "3.0.2",
}
swagger = Swagger(app)

history_data = HistoryData(configs)
dify_retriever = DifyDatasetRetriever(
    base_url=configs.get("dify_base_url", ""),
    api_key=configs.get("dify_api_key", ""),
    timeout=configs.get("dify_timeout", 15),
)
dify_kb_client = DifyKnowledgeBaseClient(
    base_url=configs.get("dify_base_url", ""),
    api_key=configs.get("dify_api_key", ""),
    timeout=configs.get("dify_timeout", 15),
)

try:
    ensure_registry_storage()
except Exception as exc:
    logger.error("Initialize merchant registry storage failed: %s", exc)

print("=" * 50)
print("Bot API Server starting...")
print(f"API docs: http://localhost:{configs['server_port']}/apidocs/")
print("=" * 50)


def _rag_asset_dir() -> Path:
    return Path(configs.get("rag_asset_dir", "src/markdata/assets")).resolve()


def _rag_asset_base_url() -> str:
    configured = str(configs.get("rag_asset_base_url") or "").strip()
    if configured:
        return configured.rstrip("/")
    return request.host_url.rstrip("/") + "/rag-assets"


def _route_assets_payload(route_result: ChatRouteModel) -> tuple[list[dict], list[dict]]:
    docs = route_result.docs if route_result else []
    images = extract_images_from_docs(docs, asset_base_url=_rag_asset_base_url())
    references = build_references(docs)
    if route_result and (route_result.rag_hit or docs):
        logger.info(
            "RAG assets parsed: merchant=%s route_type=%s rag_hit=%s docs=%s images=%s references=%s",
            route_result.merchant_id,
            route_result.route_type,
            route_result.rag_hit,
            len(docs),
            len(images),
            len(references),
        )
    return images, references


def _normalize_conversation_id(value) -> str:
    """标准化会话 ID。

    为了兼容旧客户端，缺失时统一回退到默认会话 `default`。
    """
    conversation_id = str(value or "").strip()
    return conversation_id or HistoryData.DEFAULT_CONVERSATION_ID


def _resolve_history_merchant_id(merchant_id: str, route_result: ChatRouteModel) -> str:
    """确定历史读写实际使用的商家维度。

    命中 RAG 时使用路由后的商家 ID；否则使用请求原始商家 ID。
    """
    return route_result.merchant_id if route_result and route_result.rag_hit else merchant_id


def _context_max_rounds() -> int:
    """把消息条数上限转换为轮数上限。

    由于一轮对话通常包含用户和助手两条消息，因此这里用消息数除以 2。
    """
    return HistoryData.MAX_CONTEXT_MESSAGES // 2


def _route_prompt(prompt: str, user_id: str, merchant_id: str) -> ChatRouteModel:
    """调用 Dify 路由与召回服务，决定当前请求走普通聊天还是 RAG。

    返回结果中会附带商家、检索文档、命中状态和兜底原因等信息，
    供后续上下文组装和历史写入使用。
    """
    requested_merchant = resolve_merchant(merchant_id)
    if not requested_merchant:
        return ChatRouteModel(
            requested_merchant_id=merchant_id,
            merchant_id=merchant_id,
            route_type="chat",
            rag_hit=False,
            fallback_reason="merchant_not_registered",
        )

    route_result = dify_retriever.retrieve(merchant_id=merchant_id, query=prompt)
    route_result = enrich_route_result(route_result, requested_merchant_id=merchant_id)

    if not configs.get("rag_enabled", True):
        route_result.route_type = "chat"
        route_result.rag_hit = False
        route_result.docs = []
        route_result.fallback_reason = route_result.fallback_reason or "rag_disabled"
        return route_result

    if not route_result.docs:
        route_result.route_type = "chat"
        route_result.rag_hit = False
        route_result.fallback_reason = route_result.fallback_reason or "empty_retrieval"
        return route_result

    route_result.route_type = "rag"
    route_result.rag_hit = True
    route_result.confidence = route_result.confidence or 1.0
    route_result.fallback_reason = ""
    return route_result


def _merchant_payload(merchant: dict, include_prompt: bool = False) -> dict:
    return to_public_payload(merchant, include_prompt=include_prompt)


def _json_failure(message: str, status: int = 400):
    return ResultModel.failure(message).to_json(), status


def _dify_failure(exc: DifyAPIError):
    return ResultModel(False, "dify api error", exc.payload).to_json(), exc.status_code


def _get_admin_merchant_or_error(merchant_id: str):
    merchant = merchant_registry_data.get_merchant(merchant_id, enabled_only=False)
    if not merchant:
        return None, _json_failure("merchant not registered", 404)
    if not merchant.get("dataset_id"):
        return None, _json_failure("dify_dataset_id is not configured", 400)
    return merchant, None


def _extract_update_fields(data: dict, allowed_fields: set[str]) -> dict:
    if not isinstance(data, dict) or not data:
        raise ValueError("request body is required")

    normalized = {}
    for field, value in data.items():
        target_field = "dify_dataset_id" if field == "dataset_id" else field
        if target_field not in allowed_fields:
            raise ValueError(f"unsupported update field: {field}")
        normalized[target_field] = value
    if not normalized:
        raise ValueError("update fields are required")
    return normalized


def _update_merchant_and_invalidate(merchant_id: str, fields: dict):
    merchant = merchant_registry_data.update_merchant(merchant_id, fields)
    if merchant:
        invalidate_merchant_rag_cache(merchant_id)
    return merchant


def _to_bool(value, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _to_int(value, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _to_float(value, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _build_prompt_list(
    prompt: str,
    user_id: str,
    conversation_id: str,
    merchant_id: str,
    route_result: ChatRouteModel,
) -> list[dict]:
    """构造发给大模型的完整消息列表。

    过程包括：
    1. 读取当前会话下的历史消息
    2. 根据路由结果确定商家维度
    3. 调用上下文构建器拼装 system / history / user 三部分消息
    """
    active_merchant_id = _resolve_history_merchant_id(merchant_id, route_result)
    history = history_data.get_prompts(
        user_id=user_id,
        conversation_id=conversation_id,
        merchant_id=active_merchant_id,
        max_rounds=_context_max_rounds(),
    )
    return build_messages(
        history=history,
        user_input=prompt,
        route_result=route_result,
        max_rounds=_context_max_rounds(),
    )


def _validate_request_data():
    """校验聊天接口入参。

    当前同时服务新旧两类客户端：
    - 新客户端会传 `conversation_id`
    - 旧客户端不传时自动落到默认会话
    """
    data = request.get_json() or {}
    user_id = data.get("user_id")
    conversation_id = _normalize_conversation_id(data.get("conversation_id"))
    merchant_id = (data.get("merchant_id") or "").strip()
    prompt = (data.get("prompt") or "").strip()
    if not user_id:
        return None, None, None, None, ResultModel.failure("用户ID不能为空").to_json(), 400
    if not merchant_id:
        return None, None, None, None, ResultModel.failure("商家ID不能为空").to_json(), 400
    if not prompt:
        return None, None, None, None, ResultModel.failure("请输入您的问题").to_json(), 400
    return user_id, conversation_id, merchant_id, prompt, None, None


def _validate_conversation_request_data():
    """校验新建会话接口入参。"""
    data = request.get_json() or {}
    user_id = data.get("user_id")
    merchant_id = (data.get("merchant_id") or "").strip()
    session_name = (data.get("session_name") or "").strip()
    if not user_id:
        return None, None, None, ResultModel.failure("用户ID不能为空").to_json(), 400
    if not merchant_id:
        return None, None, None, ResultModel.failure("商家ID不能为空").to_json(), 400
    return user_id, merchant_id, session_name, None, None


@app.route("/api/conversation/new", methods=["POST"])
def create_conversation():
    """创建新会话。

    会话 ID 由后端生成，前端只需要保存返回值并在后续请求中透传。
    """
    user_id = None
    try:
        user_id, merchant_id, session_name, error_body, error_status = _validate_conversation_request_data()
        if error_body:
            return error_body, error_status

        conversation = history_data.create_conversation(
            user_id=user_id,
            merchant_id=merchant_id,
            session_name=session_name,
        )
        if not conversation:
            return ResultModel.failure("会话创建失败").to_json(), 500
        return ResultModel.success(conversation).to_json(), 200
    except Exception as exc:
        logger.error("User %s create conversation failed: %s", user_id, exc)
        return ResultModel.failure(f"服务器错误：{str(exc)}").to_json(), 500


@app.route("/rag-assets/<path:filename>", methods=["GET"])
def get_rag_asset(filename: str):
    return send_from_directory(_rag_asset_dir(), filename)


@app.route("/api/chatstream", methods=["POST"])
def chat_stream():
    """流式聊天接口。

    处理流程：
    1. 校验请求
    2. 执行路由与检索
    3. 组装上下文并发起流式模型调用
    4. 在流结束后统一落库，保证历史是一问一答成对写入
    """
    t1 = time.time()
    user_id, conversation_id, merchant_id, prompt, error_body, error_status = _validate_request_data()
    if error_body:
        return error_body, error_status

    try:
        route_result = _route_prompt(prompt, user_id, merchant_id)
        active_merchant_id = _resolve_history_merchant_id(merchant_id, route_result)
        images, references = _route_assets_payload(route_result)
        prompt_list = _build_prompt_list(prompt, user_id, conversation_id, merchant_id, route_result)
        responses = Application.call(
            api_key=configs["api_key"],
            app_id=configs["app_id"],
            session_id=f"{user_id}:{conversation_id}",
            messages=prompt_list,
            stream=True,
            incremental_output=True,
        )

        def generate():
            full_response = ""
            for response in responses:
                if response.status_code != HTTPStatus.OK:
                    yield ResultModel.failure(f"API调用失败: {response.message}").to_stream()
                    return
                chunk = response.output.text
                if chunk:
                    full_response += chunk
                    yield ResultModel(success=True, message="false", data=chunk).to_stream()

            _, user_row_id, assistant_row_id = history_data.add_history(
                user_id=user_id,
                conversation_id=conversation_id,
                message=prompt,
                answer=full_response,
                merchant_id=active_merchant_id,
                route_type=route_result.route_type,
                rag_hit=route_result.rag_hit,
            )
            if images or references:
                yield ResultModel(
                    success=True,
                    message="assets",
                    data={
                        "images": images,
                        "references": references,
                    },
                ).to_stream()
            yield ResultModel(success=True, message="true", data=f"{user_row_id},{assistant_row_id}").to_stream()
            logger.info("chatstream finished in %.3fs", time.time() - t1)

        response = app.response_class(generate(), mimetype="text/event-stream")
        response.headers["Cache-Control"] = "no-cache"
        response.headers["X-Accel-Buffering"] = "no"
        return response
    except Exception as exc:
        logger.error("User %s stream chat failed: %s", user_id, exc)
        return ResultModel.failure(f"服务器错误：{str(exc)}").to_json(), 500


@app.route("/api/merchants", methods=["GET"])
def get_merchants():
    enabled_only = _to_bool(request.args.get("enabled_only"), default=True)
    include_prompt = _to_bool(request.args.get("include_prompt"), default=False)
    merchants = [
        _merchant_payload(merchant, include_prompt=include_prompt)
        for merchant in list_merchants(enabled_only=enabled_only)
    ]
    return ResultModel.success(merchants).to_json(), 200


@app.route("/api/admin/merchants", methods=["GET"])
def admin_get_merchants():
    include_prompt = _to_bool(request.args.get("include_prompt"), default=True)
    merchants = [
        _merchant_payload(merchant, include_prompt=include_prompt)
        for merchant in list_merchants(enabled_only=False)
    ]
    return ResultModel.success(merchants).to_json(), 200


@app.route("/api/admin/merchants", methods=["POST"])
def admin_create_merchant():
    data = request.get_json() or {}
    merchant_id = str(data.get("merchant_id") or "").strip()
    merchant_name = str(data.get("merchant_name") or "").strip()
    dify_dataset_id = str(data.get("dify_dataset_id") or data.get("dataset_id") or "").strip()
    dataset_name = str(data.get("dataset_name") or "").strip()
    system_prompt = str(data.get("system_prompt") or "").strip()
    top_k = _to_int(data.get("top_k"), 5)
    score_threshold = _to_float(data.get("score_threshold"), configs.get("rag_score_threshold", 0.6))

    if not merchant_id:
        return ResultModel.failure("merchant_id is required").to_json(), 400
    if not merchant_name:
        return ResultModel.failure("merchant_name is required").to_json(), 400
    if not dify_dataset_id:
        return ResultModel.failure("dify_dataset_id is required").to_json(), 400
    if merchant_registry_data.get_merchant(merchant_id, enabled_only=False):
        return ResultModel.failure("merchant_id already exists").to_json(), 409

    merchant = merchant_registry_data.create_merchant(
        merchant_id=merchant_id,
        merchant_name=merchant_name,
        dify_dataset_id=dify_dataset_id,
        dataset_name=dataset_name,
        top_k=top_k,
        score_threshold=score_threshold,
        system_prompt=system_prompt,
    )
    if not merchant:
        return ResultModel.failure("create merchant failed").to_json(), 500
    return ResultModel.success(_merchant_payload(merchant, include_prompt=True)).to_json(), 200


@app.route("/api/admin/merchants/<merchant_id>", methods=["PATCH"])
def admin_update_merchant(merchant_id: str):
    data = request.get_json() or {}
    allowed_fields = {
        "merchant_name",
        "status",
        "dify_dataset_id",
        "dataset_id",
        "dataset_name",
        "top_k",
        "score_threshold",
        "system_prompt",
    }
    try:
        fields = _extract_update_fields(data, allowed_fields)
        merchant = _update_merchant_and_invalidate(merchant_id, fields)
    except ValueError as exc:
        return _json_failure(str(exc), 400)
    except Exception as exc:
        logger.error("Update merchant %s failed: %s", merchant_id, exc)
        return ResultModel.failure(f"update merchant failed: {str(exc)}").to_json(), 500

    if not merchant:
        return ResultModel.failure("merchant not registered").to_json(), 404
    return ResultModel.success(_merchant_payload(merchant, include_prompt=True)).to_json(), 200


@app.route("/api/admin/merchants/<merchant_id>/knowledge-base-binding", methods=["PATCH"])
def admin_update_merchant_knowledge_base_binding(merchant_id: str):
    data = request.get_json() or {}
    allowed_fields = {"dify_dataset_id", "dataset_id", "dataset_name", "top_k", "score_threshold"}
    try:
        fields = _extract_update_fields(data, allowed_fields)
        merchant = _update_merchant_and_invalidate(merchant_id, fields)
    except ValueError as exc:
        return _json_failure(str(exc), 400)
    except Exception as exc:
        logger.error("Update merchant %s knowledge base binding failed: %s", merchant_id, exc)
        return ResultModel.failure(f"update knowledge base binding failed: {str(exc)}").to_json(), 500

    if not merchant:
        return ResultModel.failure("merchant not registered").to_json(), 404
    return ResultModel.success(_merchant_payload(merchant, include_prompt=True)).to_json(), 200


@app.route("/api/admin/merchants/<merchant_id>/enable", methods=["PATCH"])
def admin_enable_merchant(merchant_id: str):
    if not merchant_registry_data.get_merchant(merchant_id, enabled_only=False):
        return ResultModel.failure("merchant not registered").to_json(), 404
    if not merchant_registry_data.set_merchant_status(merchant_id, enabled=True):
        return ResultModel.failure("enable merchant failed").to_json(), 500
    invalidate_merchant_rag_cache(merchant_id)
    merchant = merchant_registry_data.get_merchant(merchant_id, enabled_only=False)
    return ResultModel.success(_merchant_payload(merchant, include_prompt=True)).to_json(), 200


@app.route("/api/admin/merchants/<merchant_id>/disable", methods=["PATCH"])
def admin_disable_merchant(merchant_id: str):
    if not merchant_registry_data.get_merchant(merchant_id, enabled_only=False):
        return ResultModel.failure("merchant not registered").to_json(), 404
    if not merchant_registry_data.set_merchant_status(merchant_id, enabled=False):
        return ResultModel.failure("disable merchant failed").to_json(), 500
    invalidate_merchant_rag_cache(merchant_id)
    merchant = merchant_registry_data.get_merchant(merchant_id, enabled_only=False)
    return ResultModel.success(_merchant_payload(merchant, include_prompt=True)).to_json(), 200


@app.route("/api/admin/merchants/<merchant_id>/dify/knowledge-base", methods=["GET"])
def admin_get_dify_knowledge_base(merchant_id: str):
    merchant, error_response = _get_admin_merchant_or_error(merchant_id)
    if error_response:
        return error_response
    try:
        data = dify_kb_client.get_dataset(merchant["dataset_id"])
        return ResultModel.success(data).to_json(), 200
    except DifyAPIError as exc:
        return _dify_failure(exc)


@app.route("/api/admin/merchants/<merchant_id>/dify/knowledge-base", methods=["PATCH"])
def admin_update_dify_knowledge_base(merchant_id: str):
    merchant, error_response = _get_admin_merchant_or_error(merchant_id)
    if error_response:
        return error_response

    payload = request.get_json() or {}
    if not payload:
        return ResultModel.failure("request body is required").to_json(), 400
    sync_local_dataset_name = _to_bool(payload.pop("sync_local_dataset_name", False), default=False)

    try:
        data = dify_kb_client.update_dataset(merchant["dataset_id"], payload)
        if sync_local_dataset_name and payload.get("name"):
            _update_merchant_and_invalidate(merchant_id, {"dataset_name": payload.get("name")})
        return ResultModel.success(data).to_json(), 200
    except ValueError as exc:
        return _json_failure(str(exc), 400)
    except DifyAPIError as exc:
        return _dify_failure(exc)


@app.route("/api/admin/merchants/<merchant_id>/dify/documents", methods=["GET"])
def admin_get_dify_documents(merchant_id: str):
    merchant, error_response = _get_admin_merchant_or_error(merchant_id)
    if error_response:
        return error_response
    try:
        data = dify_kb_client.list_documents(merchant["dataset_id"], request.args.to_dict(flat=False))
        return ResultModel.success(data).to_json(), 200
    except DifyAPIError as exc:
        return _dify_failure(exc)


@app.route("/api/admin/merchants/<merchant_id>/dify/documents/<document_id>/update-by-text", methods=["POST"])
def admin_update_dify_document_by_text(merchant_id: str, document_id: str):
    merchant, error_response = _get_admin_merchant_or_error(merchant_id)
    if error_response:
        return error_response
    payload = request.get_json() or {}
    if not payload:
        return ResultModel.failure("request body is required").to_json(), 400
    try:
        data = dify_kb_client.update_document_by_text(merchant["dataset_id"], document_id, payload)
        return ResultModel.success(data).to_json(), 200
    except DifyAPIError as exc:
        return _dify_failure(exc)


@app.route("/api/admin/merchants/<merchant_id>/dify/documents/<document_id>/update-by-file", methods=["POST"])
def admin_update_dify_document_by_file(merchant_id: str, document_id: str):
    merchant, error_response = _get_admin_merchant_or_error(merchant_id)
    if error_response:
        return error_response
    file_storage = request.files.get("file")
    if not file_storage:
        return ResultModel.failure("file is required").to_json(), 400
    try:
        fields = request.form.to_dict(flat=True)
        data = dify_kb_client.update_document_by_file(merchant["dataset_id"], document_id, file_storage, fields)
        return ResultModel.success(data).to_json(), 200
    except DifyAPIError as exc:
        return _dify_failure(exc)


@app.route("/api/admin/merchants/<merchant_id>/dify/documents/<batch>/indexing-status", methods=["GET"])
def admin_get_dify_document_indexing_status(merchant_id: str, batch: str):
    merchant, error_response = _get_admin_merchant_or_error(merchant_id)
    if error_response:
        return error_response
    try:
        data = dify_kb_client.get_indexing_status(merchant["dataset_id"], batch)
        return ResultModel.success(data).to_json(), 200
    except DifyAPIError as exc:
        return _dify_failure(exc)


@app.route(
    "/api/admin/merchants/<merchant_id>/dify/documents/<document_id>/segments/<segment_id>",
    methods=["POST"],
)
def admin_update_dify_segment(merchant_id: str, document_id: str, segment_id: str):
    merchant, error_response = _get_admin_merchant_or_error(merchant_id)
    if error_response:
        return error_response
    payload = request.get_json() or {}
    if not payload:
        return ResultModel.failure("request body is required").to_json(), 400
    try:
        data = dify_kb_client.update_segment(merchant["dataset_id"], document_id, segment_id, payload)
        return ResultModel.success(data).to_json(), 200
    except DifyAPIError as exc:
        return _dify_failure(exc)


@app.route("/api/route/resolve", methods=["POST"])
def resolve_route():
    """根据请求中的商家 ID 解析路由，返回该商家对外可用的配置快照。

    客户端在发起聊天前可调用本接口：校验 `merchant_id` 是否已注册，
    并拿到与列表接口一致的商家字段（可选是否包含提示词模板）。
    响应里同时带上调用方传入的 ID 与注册表解析后的正式 ID，并标记当前走 RAG 路由。
    """
    data = request.get_json() or {}
    merchant_id = (data.get("merchant_id") or "").strip()
    include_prompt = _to_bool(data.get("include_prompt"), default=True)
    if not merchant_id:
        return ResultModel.failure("merchant_id is required").to_json(), 400

    merchant = resolve_merchant(merchant_id)
    if not merchant:
        return ResultModel.failure("merchant not registered").to_json(), 404

    payload = _merchant_payload(merchant, include_prompt=include_prompt)
    payload["requested_merchant_id"] = merchant_id
    payload["resolved_merchant_id"] = merchant["merchant_id"]
    payload["route_type"] = "rag"
    return ResultModel.success(payload).to_json(), 200


@app.route("/api/chat", methods=["POST"])
def chat():
    """非流式聊天接口。

    与流式接口共用同一套路由、上下文和历史隔离逻辑，
    区别只在于模型结果一次性返回。
    """
    user_id, conversation_id, merchant_id, prompt, error_body, error_status = _validate_request_data()
    if error_body:
        return error_body, error_status

    try:
        route_result = _route_prompt(prompt, user_id, merchant_id)
        active_merchant_id = _resolve_history_merchant_id(merchant_id, route_result)
        images, references = _route_assets_payload(route_result)
        prompt_list = _build_prompt_list(prompt, user_id, conversation_id, merchant_id, route_result)
        response = Application.call(
            api_key=configs["api_key"],
            app_id=configs["app_id"],
            session_id=f"{user_id}:{conversation_id}",
            messages=prompt_list,
            stream=False,
        )

        if response.status_code != HTTPStatus.OK:
            return ResultModel.failure(f"API调用失败: {response.message}").to_json(), 500

        full_response = response.output.text
        _, user_row_id, assistant_row_id = history_data.add_history(
            user_id=user_id,
            conversation_id=conversation_id,
            message=prompt,
            answer=full_response,
            merchant_id=active_merchant_id,
            route_type=route_result.route_type,
            rag_hit=route_result.rag_hit,
        )
        return ResultModel.success(
            {
                "answer": full_response,
                "user_row_id": user_row_id,
                "assistant_row_id": assistant_row_id,
                "conversation_id": conversation_id,
                "merchant_id": route_result.merchant_id or merchant_id,
                "merchant_name": route_result.merchant_name,
                "dataset_id": route_result.dataset_id,
                "dataset_name": route_result.dataset_name,
                "route_type": route_result.route_type,
                "rag_hit": route_result.rag_hit,
                "fallback_reason": route_result.fallback_reason,
                "images": images,
                "references": references,
            }
        ).to_json(), 200
    except Exception as exc:
        logger.error("User %s chat failed: %s", user_id, exc)
        return ResultModel.failure(f"服务器错误：{str(exc)}").to_json(), 500


@app.route("/api/gethistory", methods=["GET"])
def get_history():
    """按用户和会话维度获取历史记录。

    保持 GET 方式以兼容现有调用方式，`conversation_id` 从 query 参数读取。
    """
    user_id = None
    try:
        user_id = request.args.get("user_id")
        conversation_id = _normalize_conversation_id(request.args.get("conversation_id"))
        merchant_id = request.args.get("merchant_id")
        if merchant_id is not None:
            merchant_id = merchant_id.strip() or None
        if not user_id:
            return ResultModel.failure("用户ID不能为空").to_json(), 400
        prompt_list = history_data.get_history(
            user_id,
            conversation_id=conversation_id,
            merchant_id=merchant_id,
        )
        return ResultModel.success(prompt_list).to_json(), 200
    except Exception as exc:
        logger.error("User %s get history failed: %s", user_id, exc)
        return ResultModel.failure(f"服务器错误：{str(exc)}").to_json(), 500


@app.route("/api/deletehistory", methods=["POST"])
def delete_history():
    """删除当前会话下的指定历史消息。"""
    user_id = None
    try:
        data = request.get_json() or {}
        user_id = data.get("user_id")
        conversation_id = _normalize_conversation_id(data.get("conversation_id"))
        history_ids = data.get("history_ids")
        if not user_id:
            return ResultModel.failure("用户ID不能为空").to_json(), 400
        if not history_ids:
            return ResultModel.failure("历史记录ID列表不能为空").to_json(), 400
        history_data.delete_history(user_id, conversation_id, history_ids)
        return ResultModel.success(f"用户 {user_id} 的对话历史已删除").to_json(), 200
    except Exception as exc:
        logger.error("User %s delete history failed: %s", user_id, exc)
        return ResultModel.failure(f"服务器错误：{str(exc)}").to_json(), 500


@app.route("/api/clearhistory", methods=["POST"])
def clear_history():
    """清空当前会话下的全部历史消息。"""
    user_id = None
    try:
        data = request.get_json() or {}
        user_id = data.get("user_id")
        conversation_id = _normalize_conversation_id(data.get("conversation_id"))
        if not user_id:
            return ResultModel.failure("用户ID不能为空").to_json(), 400
        history_data.clear_history(user_id, conversation_id)
        return ResultModel.success(f"用户 {user_id} 的对话历史已清空").to_json(), 200
    except Exception as exc:
        logger.error("User %s clear history failed: %s", user_id, exc)
        return ResultModel.failure(f"服务器错误：{str(exc)}").to_json(), 500


if __name__ == "__main__":
    # 本地直接运行时启动 Flask 服务。
    app.run(host="0.0.0.0", port=configs["server_port"], debug=True)
