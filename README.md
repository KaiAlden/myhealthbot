## 创建表的SQL

sqlite:
``` sqlite
CREATE TABLE "chat_history" (
  "id" integer PRIMARY KEY AUTOINCREMENT,
  "user_id" text NOT NULL,
  "role" text NOT NULL,
  "content" text NOT NULL,
  "time" real NOT NULL
);
```

mysql:
``` mysql
CREATE TABLE `chat_history2025` (
  `id` INT AUTO_INCREMENT PRIMARY KEY COMMENT '自增主键',
	`chat_name` VARCHAR(255) DEFAULT 'default' COMMENT '对话名称'
  `user_id` VARCHAR(255) NOT NULL COMMENT '用户ID',
  `role` ENUM('user', 'assistant', 'system') NOT NULL COMMENT '角色(user/assistant/system)',
  `content` TEXT NOT NULL COMMENT '消息内容',
  `time` TIMESTAMP NOT NULL COMMENT '时间戳(秒级精度)',
  `del_flag` TINYINT(1) DEFAULT 0 COMMENT '删除标记(0-正常,1-已删除)'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='大模型对话历史记录';
```






