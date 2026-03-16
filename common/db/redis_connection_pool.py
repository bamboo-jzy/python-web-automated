"""
Redis 连接池策略实现模块

实现 Redis 专用连接池策略，基于 redis-py 库。
支持敏感信息脱敏、SSL 配置等企业级特性。
"""

from typing import Any, Dict, cast

import redis
from redis.exceptions import ConnectionError, TimeoutError

from common.log_config import setup_logger

from .database_connection_pool_strategy import DatabaseConnectionPoolStrategy

SENSITIVE_KEYS = {"password", "auth", "secret", "token"}

logger = setup_logger()


class RedisConnectionPoolStrategy(DatabaseConnectionPoolStrategy):
    """Redis 连接池策略实现类

    特性：
    - 自动过滤无效连接参数
    - 敏感信息脱敏日志
    - SSL 配置安全校验
    - 兼容 redis-py 3.x/4.x+
    """

    def _create_pool(self, config: Dict[str, Any]) -> redis.ConnectionPool:
        """创建 Redis 连接池实例

        Args:
            config: 配置字典（含 host, port, password 等）

        Returns:
            redis.ConnectionPool: 初始化的连接池对象

        Raises:
            ValueError: 无效参数或缺失必要参数
            ConnectionError: 连接参数错误（由 redis 库抛出）
        """


        if config.get("ssl") and not config.get("ssl_cert_reqs"):
            logger.warning(
                f"[{self._base_name}] SSL 已启用但未设置 ssl_cert_reqs，建议设置为 'required' 以增强安全性"
            )

        log_config = {
            k: ("***" if k in SENSITIVE_KEYS else v) for k, v in config.items()
        }
        logger.debug(
            f"创建 Redis 连接池 | name='{self._base_name}', config={log_config}"
        )

        try:
            pool = redis.ConnectionPool(**config)
            logger.info(
                f"Redis 连接池初始化成功 | name='{self._base_name}', "
                f"host={config.get('host')}, port={config.get('port')}"
            )
            return pool
        except (ConnectionError, TimeoutError) as e:
            logger.error(f"Redis 连接测试失败 [{self._base_name}]: {e}")
            raise
        except TypeError as e:
            logger.error(f"Redis 配置参数类型错误 [{self._base_name}]: {e}")
            raise ValueError(f"无效的 Redis 配置参数: {e}") from e

    def _close_pool_impl(self) -> None:
        """安全关闭 Redis 连接池

        调用 disconnect() 断开所有连接，兼容 redis-py 多版本。
        即使连接已断开，重复调用亦安全（redis-py 内部有状态检查）。
        """
        if not hasattr(self._pool, "disconnect"):
            logger.warning(
                f"连接池对象 {type(self._pool).__name__} 无 disconnect 方法，跳过断开"
            )
            return

        try:
            pool = cast(redis.ConnectionPool, self._pool)
            pool.disconnect()
            logger.debug(f"Redis 连接池断开完成 [{self._base_name}]")
        except Exception as e:
            logger.debug(f"Redis disconnect 执行细节 [{self._base_name}]: {e}")
            raise

    def get_redis_client(self) -> redis.Redis:
        """获取 Redis 客户端实例

        Returns:
            redis.Redis: Redis 客户端实例，使用当前连接池

        Raises:
            RuntimeError: Redis 连接池已关闭时
        """
        if self.is_closed:
            raise RuntimeError(f"Redis 连接池 '{self._base_name}' 已关闭")

        pool = cast(redis.ConnectionPool, self._pool)
        return redis.Redis(connection_pool=pool)


if __name__ == "__main__":
    redis_con_pool = RedisConnectionPoolStrategy("redis")
    r = redis_con_pool.get_redis_client()
    res = r.hget("child_hash", "192.168.9.118")
    print(res)
    redis_con_pool.close_pool()
    # 支持上下文管理
