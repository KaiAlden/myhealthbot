from __future__ import annotations

from pathlib import Path

from data.chat_route_model import ChatRouteModel
from utils.config_util import get_config
from utils.mysql_util import MySQLUtil


LEGACY_REGISTRY_FILE = Path(__file__).with_name("merchant_registry.json")


def _get_db_util() -> MySQLUtil:
    config = get_config("config.json") or {}
    return MySQLUtil(
        host=config["mysql_host"],
        port=config["mysql_port"],
        user=config["mysql_user"],
        password=config["mysql_password"],
        database=config["mysql_database"],
    )


def _normalize_bool(value, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _normalize_float(value, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _normalize_int(value, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _legacy_merchants() -> list[dict]:
    config = get_config(str(LEGACY_REGISTRY_FILE)) or {}
    merchants = []
    for item in config.get("merchants", []):
        if not isinstance(item, dict):
            continue
        merchant_id = str(item.get("merchant_id") or "").strip()
        if not merchant_id:
            continue
        merchants.append(
            {
                "merchant_id": merchant_id,
                "merchant_name": str(item.get("merchant_name") or merchant_id).strip(),
                "dataset_id": str(item.get("dataset_id") or "").strip(),
                "dataset_name": str(item.get("dataset_name") or "").strip(),
                "top_k": _normalize_int(item.get("top_k"), 5),
                "score_threshold": _normalize_float(item.get("score_threshold"), 0.6),
                "system_prompt": str(item.get("system_prompt") or "").strip(),
                "enabled": _normalize_bool(item.get("enabled"), True),
            }
        )
    return merchants


def _row_to_merchant(row: tuple) -> dict:
    return {
        "merchant_id": row[0],
        "merchant_name": row[1],
        "dataset_id": row[2],
        "dataset_name": row[3],
        "top_k": row[4],
        "score_threshold": float(row[5]) if row[5] is not None else 0.6,
        "system_prompt": row[6] or "",
        "enabled": bool(row[7]),
        "status": int(row[7]),
        "created_at": row[8].isoformat() if row[8] else None,
        "updated_at": row[9].isoformat() if row[9] else None,
        "aliases": [],
        "keywords": [],
        "route_strategy": "merchant_id",
    }


class MerchantRegistryData:
    TABLE_NAME = "merchant_knowledge_base"
    UPDATE_FIELD_COLUMNS = {
        "merchant_name": "merchant_name",
        "status": "status",
        "dify_dataset_id": "dify_dataset_id",
        "dataset_name": "dataset_name",
        "top_k": "top_k",
        "score_threshold": "score_threshold",
        "system_prompt": "system_prompt",
    }

    def __init__(self):
        self.db_util = _get_db_util()

    def _ensure_table(self) -> None:
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS `{self.TABLE_NAME}` (
          `id` INT AUTO_INCREMENT PRIMARY KEY,
          `merchant_id` VARCHAR(64) NOT NULL UNIQUE,
          `merchant_name` VARCHAR(128) NOT NULL,
          `dify_dataset_id` VARCHAR(128) NOT NULL,
          `dataset_name` VARCHAR(128) DEFAULT '',
          `top_k` INT NOT NULL DEFAULT 5,
          `score_threshold` DECIMAL(5,4) NOT NULL DEFAULT 0.6000,
          `system_prompt` TEXT,
          `status` TINYINT NOT NULL DEFAULT 1,
          `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP,
          `updated_at` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          INDEX `idx_merchant_status` (`merchant_id`, `status`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
        self.db_util.cursor.execute(create_sql)
        self.db_util.connection.commit()
        self._bootstrap_from_legacy()

    def _bootstrap_from_legacy(self) -> None:
        count_sql = f"SELECT COUNT(1) FROM `{self.TABLE_NAME}`"
        rows = self.db_util.execute_query(count_sql)
        count = rows[0][0] if rows else 0
        if count:
            return

        insert_sql = (
            f"INSERT INTO `{self.TABLE_NAME}` "
            "(merchant_id, merchant_name, dify_dataset_id, dataset_name, top_k, score_threshold, system_prompt, status) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
        )
        for merchant in _legacy_merchants():
            self.db_util.execute_insert(
                insert_sql,
                (
                    merchant["merchant_id"],
                    merchant["merchant_name"],
                    merchant["dataset_id"],
                    merchant["dataset_name"],
                    merchant["top_k"],
                    merchant["score_threshold"],
                    merchant["system_prompt"],
                    1 if merchant["enabled"] else 0,
                ),
            )

    def _query(self, sql: str, params: tuple = ()) -> list[tuple]:
        self.db_util.connect()
        try:
            self._ensure_table()
            return self.db_util.execute_query(sql, params)
        finally:
            self.db_util.disconnect()

    def _execute_insert(self, sql: str, params: tuple = ()) -> int:
        self.db_util.connect()
        try:
            self._ensure_table()
            return self.db_util.execute_insert(sql, params)
        finally:
            self.db_util.disconnect()

    def _execute_update(self, sql: str, params: tuple = ()) -> int:
        self.db_util.connect()
        try:
            self._ensure_table()
            return self.db_util.execute_update(sql, params)
        finally:
            self.db_util.disconnect()

    def list_merchants(self, enabled_only: bool = True) -> list[dict]:
        sql = (
            f"SELECT merchant_id, merchant_name, dify_dataset_id, dataset_name, top_k, score_threshold, "
            f"system_prompt, status, created_at, updated_at "
            f"FROM `{self.TABLE_NAME}`"
        )
        params: tuple = ()
        if enabled_only:
            sql += " WHERE status = %s"
            params = (1,)
        sql += " ORDER BY created_at ASC, merchant_id ASC"
        return [_row_to_merchant(row) for row in self._query(sql, params)]

    def get_merchant(self, merchant_id: str | None, enabled_only: bool = False) -> dict | None:
        merchant_key = str(merchant_id or "").strip()
        if not merchant_key:
            return None

        sql = (
            f"SELECT merchant_id, merchant_name, dify_dataset_id, dataset_name, top_k, score_threshold, "
            f"system_prompt, status, created_at, updated_at "
            f"FROM `{self.TABLE_NAME}` WHERE merchant_id = %s"
        )
        params: list = [merchant_key]
        if enabled_only:
            sql += " AND status = %s"
            params.append(1)
        sql += " LIMIT 1"

        rows = self._query(sql, tuple(params))
        if not rows:
            return None
        return _row_to_merchant(rows[0])

    def create_merchant(
        self,
        merchant_id: str,
        merchant_name: str,
        dify_dataset_id: str,
        dataset_name: str = "",
        top_k: int = 5,
        score_threshold: float = 0.6,
        system_prompt: str = "",
    ) -> dict | None:
        sql = (
            f"INSERT INTO `{self.TABLE_NAME}` "
            "(merchant_id, merchant_name, dify_dataset_id, dataset_name, top_k, score_threshold, system_prompt, status) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
        )
        row_id = self._execute_insert(
            sql,
            (
                merchant_id,
                merchant_name,
                dify_dataset_id,
                dataset_name,
                top_k,
                score_threshold,
                system_prompt,
                1,
            ),
        )
        if row_id <= 0:
            return None
        return self.get_merchant(merchant_id, enabled_only=False)

    def update_merchant(self, merchant_id: str, fields: dict) -> dict | None:
        merchant_key = str(merchant_id or "").strip()
        if not merchant_key:
            raise ValueError("merchant_id is required")
        if not isinstance(fields, dict) or not fields:
            raise ValueError("update fields are required")
        if "merchant_id" in fields:
            raise ValueError("merchant_id cannot be updated")

        unknown_fields = set(fields) - set(self.UPDATE_FIELD_COLUMNS)
        if unknown_fields:
            raise ValueError(f"unsupported update fields: {', '.join(sorted(unknown_fields))}")

        if not self.get_merchant(merchant_key, enabled_only=False):
            return None

        normalized_fields = {}
        for field, value in fields.items():
            if field == "merchant_name":
                merchant_name = str(value or "").strip()
                if not merchant_name:
                    raise ValueError("merchant_name cannot be empty")
                normalized_fields[field] = merchant_name
            elif field == "dify_dataset_id":
                dataset_id = str(value or "").strip()
                if not dataset_id:
                    raise ValueError("dify_dataset_id cannot be empty")
                normalized_fields[field] = dataset_id
            elif field in {"dataset_name", "system_prompt"}:
                normalized_fields[field] = str(value or "").strip()
            elif field == "top_k":
                if isinstance(value, bool):
                    raise ValueError("top_k must be a positive integer")
                try:
                    top_k = int(value)
                except (TypeError, ValueError):
                    raise ValueError("top_k must be a positive integer")
                if top_k <= 0:
                    raise ValueError("top_k must be a positive integer")
                normalized_fields[field] = top_k
            elif field == "score_threshold":
                try:
                    score_threshold = float(value)
                except (TypeError, ValueError):
                    raise ValueError("score_threshold must be between 0 and 1")
                if score_threshold < 0 or score_threshold > 1:
                    raise ValueError("score_threshold must be between 0 and 1")
                normalized_fields[field] = score_threshold
            elif field == "status":
                if isinstance(value, bool):
                    normalized_fields[field] = 1 if value else 0
                elif value in (0, 1, "0", "1"):
                    normalized_fields[field] = int(value)
                else:
                    raise ValueError("status must be 0, 1, or bool")

        if not normalized_fields:
            raise ValueError("update fields are required")

        assignments = [
            f"`{self.UPDATE_FIELD_COLUMNS[field]}` = %s"
            for field in normalized_fields
        ]
        sql = f"UPDATE `{self.TABLE_NAME}` SET {', '.join(assignments)} WHERE merchant_id = %s"
        params = tuple(normalized_fields.values()) + (merchant_key,)
        if self._execute_update(sql, params) < 0:
            return None
        return self.get_merchant(merchant_key, enabled_only=False)

    def set_merchant_status(self, merchant_id: str, enabled: bool) -> bool:
        sql = f"UPDATE `{self.TABLE_NAME}` SET status = %s WHERE merchant_id = %s"
        return self._execute_update(sql, (1 if enabled else 0, merchant_id)) >= 0

    def ensure_initialized(self) -> None:
        self.db_util.connect()
        try:
            self._ensure_table()
        finally:
            self.db_util.disconnect()


merchant_registry_data = MerchantRegistryData()


def refresh_registry_cache() -> None:
    return None


def ensure_registry_storage() -> None:
    merchant_registry_data.ensure_initialized()


def list_merchants(enabled_only: bool = True) -> list[dict]:
    return merchant_registry_data.list_merchants(enabled_only=enabled_only)


def resolve_merchant(merchant_id: str | None, enabled_only: bool = True) -> dict | None:
    return merchant_registry_data.get_merchant(merchant_id, enabled_only=enabled_only)


def get_merchant(merchant_id: str | None) -> dict | None:
    return merchant_registry_data.get_merchant(merchant_id, enabled_only=False)


def update_merchant(merchant_id: str, fields: dict) -> dict | None:
    return merchant_registry_data.update_merchant(merchant_id, fields)


def to_public_payload(merchant: dict, include_prompt: bool = False) -> dict:
    payload = {
        "merchant_id": merchant.get("merchant_id"),
        "merchant_name": merchant.get("merchant_name"),
        "dataset_id": merchant.get("dataset_id"),
        "dataset_name": merchant.get("dataset_name"),
        "enabled": merchant.get("enabled", True),
        "aliases": merchant.get("aliases", []),
        "keywords": merchant.get("keywords", []),
        "route_strategy": merchant.get("route_strategy", "merchant_id"),
        "score_threshold": merchant.get("score_threshold", 0.6),
        "top_k": merchant.get("top_k", 5),
        "created_at": merchant.get("created_at"),
        "updated_at": merchant.get("updated_at"),
    }
    if include_prompt:
        payload["system_prompt"] = merchant.get("system_prompt", "")
    return payload


def enrich_route_result(route_result: ChatRouteModel, requested_merchant_id: str | None = None) -> ChatRouteModel:
    requested_merchant = resolve_merchant(requested_merchant_id)
    routed_merchant = resolve_merchant(route_result.merchant_id) if route_result.merchant_id else None
    final_merchant = routed_merchant or requested_merchant

    route_result.requested_merchant_id = requested_merchant_id or route_result.requested_merchant_id

    if not final_merchant:
        route_result.fallback_reason = route_result.fallback_reason or "merchant_not_registered"
        return route_result

    route_result.merchant_id = final_merchant["merchant_id"]
    route_result.merchant_name = route_result.merchant_name or final_merchant.get("merchant_name")
    route_result.dataset_id = route_result.dataset_id or final_merchant.get("dataset_id")
    route_result.dataset_name = route_result.dataset_name or final_merchant.get("dataset_name")
    route_result.system_prompt = route_result.system_prompt or final_merchant.get("system_prompt")
    return route_result
