import json
import logging
from urllib import error, request

from data.chat_route_model import ChatRouteModel


logger = logging.getLogger(__name__)


class DifyClient:
    def __init__(self, base_url: str, api_key: str, timeout: int = 15):
        self.base_url = (base_url or "").rstrip("/")
        self.api_key = api_key or ""
        self.timeout = timeout

    def route_and_retrieve(self, query: str, user_id: str, merchant_id: str) -> ChatRouteModel:
        if not self.base_url or not self.api_key:
            return ChatRouteModel(merchant_id=merchant_id, fallback_reason="dify_not_configured")

        payload = {
            "inputs": {
                "query": query,
                "user_id": str(user_id),
                "merchant_id": merchant_id,
            },
            "response_mode": "blocking",
            "user": str(user_id),
        }
        req = request.Request(
            url=f"{self.base_url}/workflows/run",
            data=json.dumps(payload).encode("utf-8"),
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
            },
            method="POST",
        )
        try:
            with request.urlopen(req, timeout=self.timeout) as response:
                body = response.read().decode("utf-8")
                data = json.loads(body)
                route = self._normalize_response(data)
                if not route.merchant_id:
                    route.merchant_id = merchant_id
                return route
        except error.HTTPError as exc:
            logger.error("Dify HTTP error %s: %s", exc.code, exc.read().decode("utf-8", errors="ignore"))
            return ChatRouteModel(merchant_id=merchant_id, fallback_reason="dify_http_error")
        except Exception as exc:
            logger.error("Dify request failed: %s", exc)
            return ChatRouteModel(merchant_id=merchant_id, fallback_reason="dify_request_failed")

    def _normalize_response(self, data: dict) -> ChatRouteModel:
        outputs = data.get("data", {}).get("outputs", {})
        if not isinstance(outputs, dict):
            return ChatRouteModel(fallback_reason="dify_invalid_response")

        route = ChatRouteModel(
            merchant_id=self._none_to_null(outputs.get("merchant_id")),
            merchant_name=self._none_to_null(outputs.get("merchant_name")),
            dataset_id=self._none_to_null(outputs.get("dataset_id")),
            dataset_name=self._none_to_null(outputs.get("dataset_name")),
            system_prompt=self._none_to_null(outputs.get("system_prompt")),
            route_type=str(outputs.get("route_type") or "chat"),
            confidence=self._to_float(outputs.get("confidence")),
            docs=self._normalize_docs(outputs.get("docs")),
            fallback_reason=str(outputs.get("fallback_reason") or ""),
            rag_hit=bool(outputs.get("rag_hit", False)),
            final_answer=self._none_to_null(outputs.get("final_answer")),
        )
        if route.route_type == "rag" and route.docs:
            route.rag_hit = True
        return route

    @staticmethod
    def _none_to_null(value):
        if value in (None, "", "none", "None", "NONE"):
            return None
        return value

    @staticmethod
    def _to_float(value) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return 0.0

    def _normalize_docs(self, docs) -> list[dict]:
        if isinstance(docs, str):
            return [{"title": "doc_1", "content": docs, "score": None}]

        normalized = []
        if not isinstance(docs, list):
            return normalized

        for index, item in enumerate(docs, start=1):
            if isinstance(item, dict):
                title = item.get("title") or item.get("name") or item.get("document_name") or f"doc_{index}"
                content = item.get("content") or item.get("text") or item.get("page_content") or item.get("segment") or ""
                score = item.get("score")
            else:
                title = f"doc_{index}"
                content = str(item)
                score = None

            if content:
                normalized.append({
                    "title": title,
                    "content": content,
                    "score": score,
                })
        return normalized
