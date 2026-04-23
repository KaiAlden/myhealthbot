import json
import logging
from decimal import Decimal
from urllib import error, request

from data.chat_route_model import ChatRouteModel
from data.merchant_registry import resolve_merchant


logger = logging.getLogger(__name__)


class DifyDatasetRetriever:
    def __init__(self, base_url: str, api_key: str, timeout: int = 15):
        self.base_url = (base_url or "").rstrip("/")
        self.api_key = api_key or ""
        self.timeout = timeout

    def retrieve(self, merchant_id: str, query: str) -> ChatRouteModel:
        merchant = resolve_merchant(merchant_id, enabled_only=True)
        if not merchant:
            return ChatRouteModel(
                requested_merchant_id=merchant_id,
                merchant_id=merchant_id,
                route_type="chat",
                rag_hit=False,
                fallback_reason="merchant_not_registered",
            )

        dataset_id = merchant.get("dataset_id")
        if not dataset_id:
            return self._fallback(merchant, "dataset_not_configured")
        if not self.base_url or not self.api_key:
            return self._fallback(merchant, "dify_not_configured")

        top_k = self._to_int(merchant.get("top_k"), 5)
        score_threshold = self._to_float(merchant.get("score_threshold"))
        score_threshold_enabled = self._to_bool(merchant.get("score_threshold_enabled"), False)
        reranking_enable = self._to_bool(merchant.get("reranking_enable"), False)

        retrieval_model = {
            "search_method": "hybrid_search",
            "top_k": top_k,
            "score_threshold_enabled": score_threshold_enabled,
            "reranking_enable": reranking_enable,
        }
        if score_threshold is not None:
            retrieval_model["score_threshold"] = score_threshold

        payload = {
            "query": query,
            "retrieval_model": retrieval_model,
        }
        req = request.Request(
            url=f"{self.base_url}/datasets/{dataset_id}/retrieve",
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
        except error.HTTPError as exc:
            logger.error(
                "Dify dataset retrieve HTTP error %s for merchant=%s: %s",
                exc.code,
                merchant_id,
                exc.read().decode("utf-8", errors="ignore"),
            )
            return self._fallback(merchant, "dify_http_error")
        except Exception as exc:
            logger.error("Dify dataset retrieve failed for merchant=%s: %s", merchant_id, exc)
            return self._fallback(merchant, "dify_request_failed")

        docs = self._normalize_docs(data.get("records"), merchant.get("score_threshold", 0.6))
        if not docs:
            return self._fallback(merchant, "empty_retrieval")

        return ChatRouteModel(
            requested_merchant_id=merchant_id,
            merchant_id=merchant["merchant_id"],
            merchant_name=merchant.get("merchant_name"),
            dataset_id=merchant.get("dataset_id"),
            dataset_name=merchant.get("dataset_name"),
            system_prompt=merchant.get("system_prompt"),
            route_type="rag",
            confidence=1.0,
            docs=docs,
            fallback_reason="",
            rag_hit=True,
        )

    def _normalize_docs(self, records, threshold: float) -> list[dict]:
        normalized = []
        if not isinstance(records, list):
            return normalized

        try:
            score_threshold = float(threshold)
        except (TypeError, ValueError):
            score_threshold = 0.6

        for index, item in enumerate(records, start=1):
            if not isinstance(item, dict):
                continue
            segment = item.get("segment") or {}
            document = segment.get("document") or {}
            content = str(segment.get("content") or "").strip()
            if not content:
                continue

            raw_score = item.get("score")
            score = self._to_float(raw_score)
            if score is not None and score < score_threshold:
                continue

            normalized.append(
                {
                    "title": document.get("name") or f"doc_{index}",
                    "content": content,
                    "score": score,
                }
            )
        return normalized

    @staticmethod
    def _to_float(value):
        if value is None:
            return None
        if isinstance(value, Decimal):
            return float(value)
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _to_int(value, default: int) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _to_bool(value, default: bool = False) -> bool:
        if value is None:
            return default
        if isinstance(value, bool):
            return value
        return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}

    @staticmethod
    def _fallback(merchant: dict, reason: str) -> ChatRouteModel:
        return ChatRouteModel(
            merchant_id=merchant.get("merchant_id"),
            merchant_name=merchant.get("merchant_name"),
            dataset_id=merchant.get("dataset_id"),
            dataset_name=merchant.get("dataset_name"),
            system_prompt=merchant.get("system_prompt"),
            route_type="chat",
            confidence=0.0,
            docs=[],
            fallback_reason=reason,
            rag_hit=False,
        )
