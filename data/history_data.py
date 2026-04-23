import datetime
import logging
import uuid

from data.prompt_model import PromptModel
from utils.mysql_util import MySQLUtil


class HistoryData:
    """历史消息与会话元数据访问层。

    这里统一负责：
    1. 会话主表 `chat_session` 的创建与写入
    2. 年度历史表 `chat_history{year}` 的建表和字段兜底
    3. 基于 `user_id + conversation_id + merchant_id` 的历史读写
    4. 仅用于拼接上下文的内存缓存维护
    """

    # 兼容旧客户端时使用的默认会话 ID。
    DEFAULT_CONVERSATION_ID = "default"
    # 未传会话名称时使用的默认标题。
    DEFAULT_SESSION_NAME = "新会话"
    # 传给模型的上下文上限，按消息条数计算，不影响数据库中的完整历史。
    MAX_CONTEXT_MESSAGES = 40

    def __init__(self, db_config: dict):
        self.logger = logging.getLogger(__name__)
        self.db_util = MySQLUtil(
            host=db_config["mysql_host"],
            port=db_config["mysql_port"],
            user=db_config["mysql_user"],
            password=db_config["mysql_password"],
            database=db_config["mysql_database"],
        )
        # 仅缓存当前进程中最近访问过的上下文消息，key 维度必须包含会话隔离信息。
        self.db_temp: dict[str, list] = {}

    @staticmethod
    def _cache_key(user_id: str, conversation_id: str, merchant_id: str | None) -> str:
        """生成上下文缓存 key。

        merchant_id 为空时统一归到 `general`，避免 None 和空字符串造成重复缓存键。
        """
        return f"{user_id}::{conversation_id}::{merchant_id or 'general'}"

    def _ensure_session_table(self):
        """确保会话主表存在。

        该表只保存会话级元数据，不保存具体消息内容。
        """
        create_sql = """
        CREATE TABLE IF NOT EXISTS `chat_session` (
          `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
          `conversation_id` VARCHAR(64) NOT NULL UNIQUE,
          `user_id` VARCHAR(64) NOT NULL,
          `merchant_id` VARCHAR(64) NOT NULL,
          `session_name` VARCHAR(128) DEFAULT '新会话',
          `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP,
          `updated_at` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          `status` TINYINT DEFAULT 1 COMMENT '1=正常 0=已删除',
          INDEX `idx_user_merchant` (`user_id`, `merchant_id`),
          INDEX `idx_conversation` (`conversation_id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
        self.db_util.cursor.execute(create_sql)
        self.db_util.connection.commit()

    def _ensure_columns(self, table_name: str):
        """给历史表补齐新增字段和索引。

        这里采用“尝试执行，失败回滚”的方式做幂等兜底，
        兼容已经存在的老表结构。
        """
        alter_sql_list = [
            f"ALTER TABLE `{table_name}` ADD COLUMN `merchant_id` VARCHAR(64) DEFAULT NULL COMMENT 'merchant id'",
            (
                f"ALTER TABLE `{table_name}` ADD COLUMN `conversation_id` VARCHAR(64) NOT NULL "
                f"DEFAULT '{self.DEFAULT_CONVERSATION_ID}' AFTER `user_id`"
            ),
            f"ALTER TABLE `{table_name}` ADD COLUMN `route_type` VARCHAR(32) DEFAULT 'chat' COMMENT 'route type'",
            f"ALTER TABLE `{table_name}` ADD COLUMN `rag_hit` TINYINT(1) DEFAULT 0 COMMENT 'rag hit'",
        ]
        for sql in alter_sql_list:
            try:
                self.db_util.cursor.execute(sql)
                self.db_util.connection.commit()
            except Exception:
                self.db_util.connection.rollback()

        index_sql_list = [
            f"ALTER TABLE `{table_name}` ADD INDEX `idx_user_merchant_time` (`user_id`, `merchant_id`, `time`)",
            (
                f"ALTER TABLE `{table_name}` ADD INDEX `idx_conv_user_merchant_time` "
                f"(`conversation_id`, `user_id`, `merchant_id`, `time`)"
            ),
        ]
        for sql in index_sql_list:
            try:
                self.db_util.cursor.execute(sql)
                self.db_util.connection.commit()
            except Exception:
                self.db_util.connection.rollback()

    def _get_or_create_table(self, year: int | None = None) -> str:
        """获取当前年份历史表，不存在则自动创建。

        同时会先确保 `chat_session` 存在，避免新增会话接口调用时缺表。
        """
        if year is None:
            year = datetime.datetime.now().year
        table_name = f"chat_history{year}"

        self.db_util.connect()
        try:
            self._ensure_session_table()
            if not self.db_util.table_exists(table_name):
                create_sql = f"""
                CREATE TABLE `{table_name}` (
                  `id` INT AUTO_INCREMENT PRIMARY KEY,
                  `chat_name` VARCHAR(255) DEFAULT 'default',
                  `user_id` VARCHAR(255) NOT NULL,
                  `conversation_id` VARCHAR(64) NOT NULL DEFAULT '{self.DEFAULT_CONVERSATION_ID}',
                  `role` ENUM('user', 'assistant', 'system') NOT NULL,
                  `content` TEXT NOT NULL,
                  `time` TIMESTAMP NOT NULL,
                  `del_flag` TINYINT(1) DEFAULT 0,
                  `merchant_id` VARCHAR(64) DEFAULT NULL,
                  `route_type` VARCHAR(32) DEFAULT 'chat',
                  `rag_hit` TINYINT(1) DEFAULT 0
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
                self.db_util.cursor.execute(create_sql)
                self.db_util.cursor.execute(
                    f"""
                    ALTER TABLE `{table_name}`
                    ADD INDEX `idx_user_time` (`user_id`, `time`),
                    ADD INDEX `idx_del_flag` (`del_flag`),
                    ADD INDEX `idx_user_merchant_time` (`user_id`, `merchant_id`, `time`),
                    ADD INDEX `idx_conv_user_merchant_time` (`conversation_id`, `user_id`, `merchant_id`, `time`)
                    """
                )
                self.db_util.connection.commit()
            else:
                self._ensure_columns(table_name)
            return table_name
        finally:
            self.db_util.disconnect()

    def create_conversation(
        self,
        user_id: str,
        merchant_id: str,
        session_name: str | None = None,
    ) -> dict | None:
        """创建一个新的聊天会话。

        conversation_id 始终由后端生成，前端只负责保存并在后续请求中透传。
        """
        conversation_id = str(uuid.uuid4())
        now = datetime.datetime.now()
        final_session_name = (session_name or self.DEFAULT_SESSION_NAME).strip() or self.DEFAULT_SESSION_NAME
        sql = (
            "INSERT INTO chat_session "
            "(conversation_id, user_id, merchant_id, session_name, created_at, updated_at, status) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s)"
        )
        try:
            self._get_or_create_table()
        except Exception as exc:
            self.logger.error("Prepare conversation table failed: %s", exc)
            return None

        self.db_util.connect()
        try:
            row_id = self.db_util.execute_insert(
                sql,
                (conversation_id, user_id, merchant_id, final_session_name, now, now, 1),
            )
            if row_id <= 0:
                return None
            return {
                "conversation_id": conversation_id,
                "session_name": final_session_name,
                "created_at": now.isoformat(),
            }
        except Exception as exc:
            self.logger.error("Create conversation failed: %s", exc)
            return None
        finally:
            self.db_util.disconnect()

    def add_history(
        self,
        user_id: str,
        conversation_id: str,
        message: str,
        answer: str,
        merchant_id: str | None = None,
        route_type: str = "chat",
        rag_hit: bool = False,
    ) -> tuple[bool, int, int]:
        """写入一轮对话历史。

        一次调用会插入两条记录：
        - 用户消息
        - 助手回复

        写入成功后同步刷新当前会话的内存缓存，减少下一次上下文读取的数据库压力。
        """
        try:
            table_name = self._get_or_create_table()
        except Exception as exc:
            self.logger.error("Create history table failed: %s", exc)
            return False, -1, -1

        sql = (
            f"INSERT INTO {table_name} "
            f"(user_id, conversation_id, role, content, time, chat_name, del_flag, merchant_id, route_type, rag_hit) "
            f"VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        )
        self.db_util.connect()
        try:
            now = datetime.datetime.now()
            user_row_id = self.db_util.execute_insert(
                sql,
                (
                    user_id,
                    conversation_id,
                    "user",
                    message,
                    now,
                    "default",
                    0,
                    merchant_id,
                    route_type,
                    int(rag_hit),
                ),
            )
            assistant_row_id = self.db_util.execute_insert(
                sql,
                (
                    user_id,
                    conversation_id,
                    "assistant",
                    answer,
                    now,
                    "default",
                    0,
                    merchant_id,
                    route_type,
                    int(rag_hit),
                ),
            )

            cache_key = self._cache_key(user_id, conversation_id, merchant_id)
            if cache_key not in self.db_temp:
                self.db_temp[cache_key] = []
            self.db_temp[cache_key].append(PromptModel(role="user", content=message).to_Prompt())
            self.db_temp[cache_key].append(PromptModel(role="assistant", content=answer).to_Prompt())
            if len(self.db_temp[cache_key]) > self.MAX_CONTEXT_MESSAGES:
                self.db_temp[cache_key] = self.db_temp[cache_key][-self.MAX_CONTEXT_MESSAGES :]

            return user_row_id > 0 and assistant_row_id > 0, user_row_id, assistant_row_id
        except Exception as exc:
            self.logger.error("Add history failed: %s", exc)
            return False, -1, -1
        finally:
            self.db_util.disconnect()

    def get_prompts(
        self,
        user_id: str,
        conversation_id: str,
        merchant_id: str | None = None,
        max_rounds: int = 8,
    ) -> list[dict]:
        """获取传给模型的上下文消息列表。

        先从进程内缓存读取，缓存不存在时再查数据库。
        最终返回的数据只保留最近 `MAX_CONTEXT_MESSAGES` 条，
        用于控制模型上下文长度。
        """
        cache_key = self._cache_key(user_id, conversation_id, merchant_id)
        if cache_key not in self.db_temp:
            self.get_history(
                user_id,
                conversation_id=conversation_id,
                merchant_id=merchant_id,
                max_rounds=max_rounds,
            )
        history = self.db_temp.get(cache_key, [])
        if len(history) > self.MAX_CONTEXT_MESSAGES:
            history = history[-self.MAX_CONTEXT_MESSAGES :]
        return history

    def get_history(
        self,
        user_id: str,
        conversation_id: str = DEFAULT_CONVERSATION_ID,
        merchant_id: str | None = None,
        max_rounds: int | None = None,
    ) -> list[dict]:
        """查询指定会话下的历史消息。

        这里返回的是前端展示用历史结构；同时会把对应结果写入内存缓存，
        供后续 `get_prompts()` 直接复用。
        """
        try:
            table_name = self._get_or_create_table()
        except Exception as exc:
            self.logger.error("Prepare history table failed: %s", exc)
            return []

        sql = f"""
        SELECT id, user_id, role, content, time
        FROM {table_name}
        WHERE user_id = %s
          AND conversation_id = %s
          AND del_flag = 0
          AND ((%s IS NULL AND merchant_id IS NULL) OR merchant_id = %s)
        ORDER BY id DESC
        """
        self.db_util.connect()
        try:
            rows = self.db_util.execute_query(sql, (user_id, conversation_id, merchant_id, merchant_id))
            if max_rounds:
                rows = rows[: max_rounds * 2]
            rows = list(reversed(rows))

            prompt_list = [
                PromptModel(id=row[0], user_id=row[1], role=row[2], content=row[3], time=row[4])
                for row in rows
            ]
            self.db_temp[self._cache_key(user_id, conversation_id, merchant_id)] = [
                prompt.to_Prompt() for prompt in prompt_list
            ]
            return [prompt.to_history() for prompt in prompt_list]
        except Exception as exc:
            self.logger.error("Get history failed: %s", exc)
            return []
        finally:
            self.db_util.disconnect()

    def delete_history(self, user_id: str, conversation_id: str, history_ids: list) -> bool:
        """按消息 ID 逻辑删除指定会话下的历史记录。

        删除条件会同时限定 user_id 和 conversation_id，
        避免误删同一用户其他聊天窗口的数据。
        """
        if not history_ids:
            return True
        try:
            table_name = self._get_or_create_table()
        except Exception as exc:
            self.logger.error("Prepare history table failed: %s", exc)
            return False

        placeholders = ",".join(["%s" for _ in history_ids])
        sql = (
            f"UPDATE {table_name} SET del_flag = 1 "
            f"WHERE user_id = %s AND conversation_id = %s AND id IN ({placeholders})"
        )
        params = [user_id, conversation_id] + history_ids

        self.db_util.connect()
        try:
            res = self.db_util.execute_update(sql, params)
            self._clear_user_cache(user_id, conversation_id)
            return res >= 0
        except Exception as exc:
            self.logger.error("Delete history failed: %s", exc)
            return False
        finally:
            self.db_util.disconnect()

    def clear_history(self, user_id: str, conversation_id: str) -> bool:
        """清空指定会话下的全部历史消息。

        这里只清理消息表，不删除 `chat_session` 中的会话元数据，
        这样前端仍然可以保留当前会话对象。
        """
        try:
            table_name = self._get_or_create_table()
        except Exception as exc:
            self.logger.error("Prepare history table failed: %s", exc)
            return False

        sql = f"UPDATE {table_name} SET del_flag = 1 WHERE user_id = %s AND conversation_id = %s"
        self.db_util.connect()
        try:
            res = self.db_util.execute_update(sql, (user_id, conversation_id))
            self._clear_user_cache(user_id, conversation_id)
            return res >= 0
        except Exception as exc:
            self.logger.error("Clear history failed: %s", exc)
            return False
        finally:
            self.db_util.disconnect()

    def _clear_user_cache(self, user_id: str, conversation_id: str):
        """清除某个具体会话对应的上下文缓存。"""
        for cache_key in list(self.db_temp.keys()):
            if cache_key.startswith(f"{user_id}::{conversation_id}::"):
                del self.db_temp[cache_key]
