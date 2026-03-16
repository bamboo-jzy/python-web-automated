"""
数据库连接池工厂模块

该模块提供了一个统一的工厂类 (DatabaseConnectionPool)，
用于根据数据库类型创建和管理不同的连接池实例（如 MySQL, Redis）。

"""
from typing import Optional

from common.db.mysql_connection_pool import MySQLConnectionPoolStrategy
from common.db.redis_connection_pool import RedisConnectionPoolStrategy


class DatabaseConnectionPool:
    """数据库连接池工厂类

    该类充当策略模式的上下文，根据传入的数据库类型返回相应的连接池实例。
    它封装了具体类的实例化过程，提供了一个统一的接口来获取连接池。

    属性：
        _STRATEGY_MAP (Dict[str, Type]): 数据库类型到连接池类的映射表
    """

    # 策略映射表：键为数据库类型，值为对应的连接池类
    _STRATEGY_MAP = {"mysql": MySQLConnectionPoolStrategy, "redis": RedisConnectionPoolStrategy}

    @staticmethod
    def get_connection_pool(
            database_type: str,
            base_name: str,
            config_file_path: Optional[str] = None
    ):
        """获取指定类型的数据库连接池实例

        根据数据库类型字符串查找对应的连接池类，并初始化返回其实例。

        Args:
            database_type (str): 数据库类型标识符，支持 'mysql' 或 'redis'
            base_name (str): 配置文件中对应的具体配置名称（键名）
            config_file (str, optional): 自定义配置文件路径。默认为None，使用类内默认值

        Returns:
            Union[MysqlConnectionPool, RedisConnectionPool]: 初始化后的连接池实例

        Raises:
            ValueError: 当请求的数据库类型不被支持时
        """
        strategy_class = DatabaseConnectionPool._STRATEGY_MAP.get(database_type)
        if not strategy_class:
            raise ValueError(f"不支持的数据库类型：{database_type}")
        return strategy_class(base_name, config_file_path)


if __name__ == "__main__":
    # 测试 MySQL 连接池
    mysql_pool = DatabaseConnectionPool.get_connection_pool("mysql", "mysql")
    res = mysql_pool.select_database("select * from user;")
    # res = mysql_pool.change_database("具体sql")
    print(res)
    mysql_pool.close_pool()

    # 测试 Redis 连接池
    redis_pool = DatabaseConnectionPool.get_connection_pool("redis", "redis")
    r = redis_pool.get_redis_client()
    res = r.hget("child_hash", "192.168.9.118")
    print(res)
    redis_pool.close_pool()