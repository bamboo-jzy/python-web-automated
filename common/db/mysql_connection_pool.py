"""
MySQL 连接池策略实现模块

基于 DBUtils.PooledDB + PyMySQL 实现连接池。
支持配置前缀分离、敏感信息脱敏
"""

from typing import Any, Dict, Generator, List, Optional, Union, cast

import pymysql
from dbutils.pooled_db import PooledDB

from common.db.database_connection_pool_strategy import DatabaseConnectionPoolStrategy
from common.log_config import setup_logger

logger = setup_logger()

SENSITIVE_KEYS = {"password", "passwd", "secret", "token"}


class MySQLConnectionPoolStrategy(DatabaseConnectionPoolStrategy):
    """MySQL 连接池策略实现（PooledDB + PyMySQL）"""

    def _create_pool(self, config: Dict[str, Any]) -> PooledDB:
        """
        创建 MySQL 连接池

        配置约定：
        - 连接池参数：以 `pool_` 为前缀（如 pool_maxconnections）
        - 数据库参数：直接使用（host, port, user, password...）

        Args:
            config: 混合配置字典

        Returns:
            PooledDB: 初始化完成的连接池实例

        Raises:
            ValueError: 配置缺失或无效
            pymysql.MySQLError: 数据库连接参数错误
        """
        pool_kwargs = {}
        db_kwargs = {}
        for key, value in config.items():
            if key.startswith("pool_"):
                pool_kwargs[key[5:]] = value
            else:
                db_kwargs[key] = value

        required = {"host", "user", "database"}
        missing = required - db_kwargs.keys()
        if missing:
            raise ValueError(f"MySQL 配置缺失必要参数: {missing}")

        log_db = {
            k: ("***" if k in SENSITIVE_KEYS else v) for k, v in db_kwargs.items()
        }
        log_pool = pool_kwargs.copy()
        if "password" in log_pool:
            log_pool["password"] = "***"

        logger.debug(
            f"创建 MySQL 连接池 | name='{self._base_name}', "
            f"pool_config={log_pool}, db_config={log_db}"
        )

        try:
            pool = PooledDB(creator=pymysql, **pool_kwargs, **db_kwargs)
            logger.info(
                f"MySQL 连接池初始化成功 | name='{self._base_name}', "
                f"pool_size={pool_kwargs.get('maxconnections', 'N/A')}"
            )
            return pool
        except (pymysql.err.OperationalError, pymysql.err.ProgrammingError) as e:
            logger.error(f"MySQL 连接参数错误 [{self._base_name}]: {e}")
            raise ValueError(f"无效的 MySQL 连接配置: {e}") from e
        except TypeError as e:
            logger.error(f"PooledDB 参数错误 [{self._base_name}]: {e}")
            raise ValueError(f"连接池配置参数无效: {e}") from e
        except Exception as _:
            logger.exception(f"MySQL 连接池创建异常 [{self._base_name}]")
            raise

    def _close_pool_impl(self) -> None:
        """
        安全关闭 PooledDB 连接池
        """
        pool = cast(PooledDB, self._pool)
        assert pool is not None, "内部错误：_close_pool_impl 被调用时连接池应已初始化"

        try:
            pool.close()
            logger.debug(f"MySQL 连接池已关闭 [{self._base_name}]")
        except Exception as e:
            logger.debug(f"PooledDB close 执行细节 [{self._base_name}]: {e}")
            raise

    def select_database(
        self,
        sql: str,
        params: Optional[tuple] = None,
    ) -> List[dict]:
        """执行数据库查询操作

        执行SELECT语句并返回查询结果，结果以字典列表形式返回。

        Args:
            sql (str): SQL查询语句
            params (Optional[tuple], optional): SQL参数元组。默认为None

        Returns:
            List[dict]: 查询结果列表，每个元素为一个字典，代表一行数据

        Raises:
            pymysql.MySQLError: 当数据库查询失败时
            Exception: 当执行查询时发生未知错误时
        """
        logger.debug(f"执行查询 [select_database]: {sql}, 参数: {params}")

        try:
            with cast(PooledDB, self._pool).connection() as conn:
                with conn.cursor(cursor=pymysql.cursors.DictCursor) as cursor:
                    execute_time = cursor.execute(sql, params)
                    logger.debug(f"SQL 查询成功执行，影响/获取行数: {execute_time}")

                    results = cursor.fetchall()
                    logger.debug(f"查询完成，共获取 {len(results)} 条记录")
                    return results

        except pymysql.MySQLError as e:
            error_msg = f"数据库查询失败: {e}, 执行的SQL: {sql}, 使用的参数: {params}"
            logger.error(error_msg)
            raise pymysql.MySQLError(error_msg) from e
        except Exception as e:
            error_msg = f"执行查询时发生未知错误: {e}"
            logger.exception(error_msg)
            raise

    def select_large_database(
        self, sql: str, params: Optional[tuple] = None, batch_size: int = 1000
    ) -> Generator[List[dict], None, None]:
        """流式读取大量数据

        用于处理大数据量查询，通过生成器分批返回结果，避免内存溢出。

        Args:
            sql (str): SQL查询语句
            params (Optional[tuple], optional): SQL参数元组。默认为None
            batch_size (int, optional): 每批次返回的记录数。默认为1000

        Yields:
            List[dict]: 每次生成一个批次的查询结果，以字典列表形式返回

        Raises:
            pymysql.MySQLError: 当分批查询数据库失败时
            Exception: 当执行分批查询时发生未知错误时
        """
        logger.debug(
            f"开始流式查询 [select_large_database]，SQL: {sql}, 参数: {params}, 批次大小: {batch_size}"
        )

        try:
            with cast(PooledDB, self._pool).connection() as conn:
                with conn.cursor(cursor=pymysql.cursors.DictCursor) as cursor:
                    cursor.execute(sql, params)
                    logger.info("流式查询游标已就绪，开始分批生成数据...")

                    batch_num = 0
                    while True:
                        batch_num += 1
                        batch = cursor.fetchmany(batch_size)

                        if not batch:
                            logger.info(f"流式查询结束，共处理 {batch_num - 1} 个批次")
                            break

                        logger.debug(
                            f"流式查询生成第 {batch_num} 批数据，数量: {len(batch)}"
                        )
                        yield batch

        except pymysql.MySQLError as e:
            error_msg = f"分批查询数据库失败: {e}, SQL: {sql}, 参数: {params}"
            logger.error(error_msg)
            raise pymysql.MySQLError(error_msg) from e
        except Exception as e:
            error_msg = f"执行分批查询时发生未知错误: {e}"
            logger.exception(error_msg)
            raise

    def change_database(
        self,
        sql: str,
        params: Optional[Union[tuple, List[tuple]]] = None,
        batch_size: int = 1000,
    ) -> int:
        """执行数据库变更操作

        执行INSERT、UPDATE、DELETE等数据变更操作，支持单条和批量执行。

        Args:
            sql (str): SQL变更语句
            params (Optional[Union[tuple, List[tuple]]], optional): SQL参数，
                可以是单个元组或元组列表。默认为None
            batch_size (int, optional): 批量操作时的批次大小。默认为1000

        Returns:
            int: 受影响的行数

        Raises:
            TypeError: 当params参数类型错误时
            pymysql.MySQLError: 当数据库变更失败时
            Exception: 当执行数据库变更时发生未知错误时
        """
        operation_type = "批量更新" if isinstance(params, list) else "单条变更"
        logger.debug(
            f"执行数据库变更 [{operation_type}]: {sql}, 参数示例: {params[:3] if isinstance(params, list) else params}"
        )

        try:
            with cast(PooledDB, self._pool).connection() as conn:
                with conn.cursor() as cursor:
                    affected = 0

                    if params is None:
                        affected = cursor.execute(sql)
                        logger.debug(f"无参数执行，影响行数: {affected}")

                    elif isinstance(params, tuple):
                        affected = cursor.execute(sql, params)
                        logger.debug(f"单条执行完成，影响行数: {affected}")

                    elif isinstance(params, list):
                        if not params:
                            logger.debug("传入的参数列表为空，跳过执行")
                            return 0

                        total_params = len(params)
                        logger.info(
                            f"检测到 {total_params} 条待处理数据，开始分批次提交 (批次大小: {batch_size})"
                        )

                        for i in range(0, total_params, batch_size):
                            batch_params = params[i : i + batch_size]
                            batch_affected = cursor.executemany(sql, batch_params)
                            affected += batch_affected
                            logger.debug(
                                f"批次提交完成 [{i // batch_size + 1}]，本批次影响: {batch_affected}，累计: {affected}"
                            )

                    else:
                        err_msg = f"参数params类型错误，需为None、元组或元组列表，当前类型: {type(params)}"
                        logger.error(err_msg)
                        raise TypeError(err_msg)

                    conn.commit()
                    logger.info(f"事务提交成功，总影响行数: {affected}")
                    return affected

        except pymysql.MySQLError as e:
            error_msg = f"数据库变更失败: {str(e)}，执行SQL: {sql}"
            logger.error(error_msg)
            logger.warning("数据库变更异常，事务已自动回滚")
            raise pymysql.MySQLError(error_msg) from e

        except Exception as e:
            error_msg = f"执行数据库变更时发生未知错误: {str(e)}"
            logger.exception(error_msg)
            raise


if __name__ == "__main__":
    mysql_con_pool = MySQLConnectionPoolStrategy("mysql")
    res = mysql_con_pool.select_database("select * from user;")
    # res = mysql_pool.change_database("具体sql")
    print(res)
    mysql_con_pool.close_pool()
