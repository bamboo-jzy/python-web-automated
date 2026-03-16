"""
数据库连接池策略抽象基类模块

该模块定义了数据库连接池的抽象基类 (DatabaseConnectionPoolStrategy)。
采用策略模式与模板方法模式，封装连接池的通用生命周期管理流程，
强制子类实现具体创建/关闭逻辑，支持多数据库类型扩展。
"""

import traceback
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

from common.file_data_reader import FileDataReader
from common.log_config import setup_logger

logger = setup_logger()


class DatabaseConnectionPoolStrategy(ABC):
    """数据库连接池策略抽象基类

    封装连接池初始化、验证、关闭的标准流程（模板方法模式），
    子类仅需实现具体数据库驱动的创建与关闭逻辑（策略模式）。
    支持上下文管理器与状态查询，提升资源管理安全性与可观察性。

    属性:
        _base_name (str): 配置键名（标识具体数据库实例）
        _config_file_path (str): 配置文件路径
        _pool (Optional[Any]): 连接池实例（None 表示未初始化或已关闭）
    """

    DEFAULT_CONFIG_FILE = "config/database.toml"

    def __init__(
        self,
        base_name: str,
        config_file_path: Optional[str] = None
    ):
        """初始化连接池策略（立即初始化模式）

        Args:
            base_name: 配置文件中目标数据库配置块的键名
            config_file_path: 配置文件路径。若为 None 或空字符串，使用 DEFAULT_CONFIG_FILE

        Raises:
            FileNotFoundError: 配置文件不存在
            KeyError: 配置中缺失 base_name 对应的配置块
            ValueError: 配置内容非字典或为空
            RuntimeError: 连接池创建过程中发生不可恢复错误
        """
        if not base_name or not isinstance(base_name, str):
            raise ValueError("base_name 必须为非空字符串")

        self._base_name = base_name.strip()
        self._config_file_path = (config_file_path or self.DEFAULT_CONFIG_FILE).strip()
        self._pool: Optional[Any] = None
        self._initialize_pool()

    def _initialize_pool(self) -> None:
        """模板方法：标准化初始化流程（配置加载 → 验证 → 创建）"""
        try:
            logger.debug(
                f"加载配置: file='{self._config_file_path}', section='{self._base_name}'"
            )
            reader = FileDataReader(self._config_file_path)
            _, full_config = reader.read()

            if self._base_name not in full_config:
                err_msg = f"配置缺失: section '{self._base_name}' 不存在于 {self._config_file_path}"
                logger.error(err_msg)
                raise KeyError(err_msg)

            base_config = full_config[self._base_name]
            if not isinstance(base_config, dict) or not base_config:
                err_msg = f"配置无效: section '{self._base_name}' 非字典或为空"
                logger.error(err_msg)
                raise ValueError(err_msg)

            self._pool = self._create_pool(base_config)
            logger.info(
                f"连接池初始化成功 | name='{self._base_name}', "
                f"type={type(self._pool).__name__}, config_keys={list(base_config.keys())}"
            )

        except (FileNotFoundError, KeyError, ValueError) as e:
            logger.error(
                f"连接池初始化失败 [{self._base_name}]: {type(e).__name__} - {e}",
                exc_info=True,
            )
            raise
        except Exception as e:
            logger.critical(
                f"连接池初始化发生未预期异常 [{self._base_name}]: {e}\n{traceback.format_exc()}",
                exc_info=True,
            )
            raise RuntimeError(f"连接池初始化失败: {e}") from e

    @abstractmethod
    def _create_pool(self, config: Dict[str, Any]) -> Any:
        """子类实现：创建具体数据库连接池实例

        Args:
            config: 已验证的配置字典（含 host, port, user, password 等）

        Returns:
            初始化完成的连接池对象

        Note:
            - 子类应处理驱动特定的参数转换与异常
            - 建议对敏感信息（如密码）在日志中脱敏
        """
        pass

    def close_pool(self) -> None:
        """安全关闭连接池（幂等操作）

        - 若连接池已关闭/未初始化，静默跳过
        - 关闭异常仅记录，不中断流程，确保状态重置
        - 最终将 _pool 置为 None 保证状态一致性
        """
        if self._pool is None:
            logger.debug(f"连接池 '{self._base_name}' 已关闭或未初始化，跳过关闭操作")
            return

        try:
            self._close_pool_impl()
            logger.info(f"连接池 '{self._base_name}' 已安全关闭")
        except Exception as e:
            logger.error(
                f"关闭连接池时发生异常 [{self._base_name}]: {e}", exc_info=True
            )
        finally:
            self._pool = None

    @abstractmethod
    def _close_pool_impl(self) -> None:
        """子类实现：执行底层连接池关闭逻辑

        Note:
            - 实现应具备幂等性（重复调用安全）
            - 推荐调用驱动原生的 close()/terminate() 方法
            - 避免在此方法中抛出未处理异常（由 close_pool 统一捕获）
        """
        pass

    @property
    def is_closed(self) -> bool:
        """连接池状态查询：是否已关闭或未初始化"""
        return self._pool is None

    def __enter__(self) -> "DatabaseConnectionPoolStrategy":
        """支持上下文管理器：with pool_strategy as pool: ..."""
        if self.is_closed:
            raise RuntimeError(f"连接池 '{self._base_name}' 已关闭，无法进入上下文")
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> bool:
        """退出上下文时自动关闭连接池"""
        self.close_pool()
        return False

    def __del__(self) -> None:
        """析构时尝试清理资源（辅助保障，不替代显式关闭）"""
        if not self.is_closed:
            logger.warning(
                f"连接池 '{self._base_name}' 未显式关闭，触发析构清理。"
                "建议使用 close_pool() 或上下文管理器确保资源释放。"
            )
            try:
                self.close_pool()
            except Exception as e:
                logger.debug(f"析构时关闭连接池异常: {e}")
