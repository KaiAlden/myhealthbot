import mysql.connector
import logging
from typing import List, Tuple, Any, Optional

# 配置日志
logger = logging.getLogger('mysql_util')

class MySQLUtil:
    """
    MySQL数据库工具类
    提供常用的数据库操作方法
    """
    
    def __init__(self, host: str, port: int, user: str, password: str, database: str):
        """
        初始化数据库连接参数
        Args:
            host (str): 数据库主机地址
            port (int): 数据库端口
            user (str): 数据库用户名
            password (str): 数据库密码
            database (str): 数据库名称
        """
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        
        self.connection = None
        self.cursor = None
    
    def connect(self) -> bool:
        """
        建立数据库连接
        Returns:
            bool: 连接成功返回True，否则返回False
        """
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database
            )
            self.cursor = self.connection.cursor()
            return True
        except mysql.connector.Error as e:
            logger.error(f"连接MySQL数据库失败: {e}")
            return False
    
    def disconnect(self):
        """关闭数据库连接"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
            logger.info("MySQL数据库连接已关闭")
    
    def execute_query(self, query: str, params: Tuple = ()) -> List[Tuple]:
        """
        执行查询语句
        Args:
            query (str): SQL查询语句
            params (Tuple): 查询参数
        Returns:
            List[Tuple]: 查询结果
        """
        try:
            self.cursor.execute(query, params)
            results = self.cursor.fetchall()
            return results
        except mysql.connector.Error as e:
            logger.error(f"执行查询失败: {e}")
            return []
    
    def execute_update(self, sql: str, params: Tuple = ()) -> int:
        """
        执行更新语句（UPDATE, DELETE）
        Args:
            sql (str): SQL更新语句
            params (Tuple): 更新参数
        Returns:
            int: 影响的行数
        """
        try:
            self.cursor.execute(sql, params)
            self.connection.commit()
            row_count = self.cursor.rowcount
            return row_count
        except mysql.connector.Error as e:
            self.connection.rollback()
            logger.error(f"执行更新失败: {e}")
            return -1

    def execute_insert(self, sql: str, params: Tuple = ()) -> int:
        """
        执行插入语句并返回新记录的ID
        Args:
            sql (str): SQL插入语句
            params (Tuple): 插入参数
        Returns:
            int: 新插入记录的ID，如果插入失败则返回-1
        """
        try:
            self.cursor.execute(sql, params)
            self.connection.commit()
            last_row_id = self.cursor.lastrowid
            return last_row_id
        except mysql.connector.Error as e:
            self.connection.rollback()
            logger.error(f"执行插入失败: {e}")
            return -1
    
    def execute_many(self, query: str, params_list: List[Tuple]) -> int:
        """
        批量执行SQL语句
        Args:
            query (str): SQL语句
            params_list (List[Tuple]): 参数列表
        Returns:
            int: 影响的行数
        """
        try:
            self.cursor.executemany(query, params_list)
            self.connection.commit()
            row_count = self.cursor.rowcount
            return row_count
        except mysql.connector.Error as e:
            self.connection.rollback()
            logger.error(f"批量执行失败: {e}")
            return -1
    
    def create_table(self, table_name: str, columns: List[str]) -> bool:
        """
        创建表
        Args:
            table_name (str): 表名
            columns (List[str]): 列定义列表
            
        Returns:
            bool: 创建成功返回True，否则返回False
        """
        try:
            column_defs = ', '.join(columns)
            query = f"CREATE TABLE IF NOT EXISTS {table_name} ({column_defs})"
            self.cursor.execute(query)
            self.connection.commit()
            return True
        except mysql.connector.Error as e:
            logger.error(f"创建表失败: {e}")
            return False
    
    def table_exists(self, table_name: str) -> bool:
        """
        检查表是否存在
        Args:
            table_name (str): 表名
        Returns:
            bool: 存在返回True，否则返回False
        """
        query = "SELECT table_name FROM information_schema.tables WHERE table_schema = %s AND table_name = %s"
        result = self.execute_query(query, (self.database, table_name))
        return len(result) > 0
    
    def __enter__(self):
        """上下文管理器入口"""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口"""
        self.disconnect()