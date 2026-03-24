"""
日志处理器模块

提供一个标准化的日志配置函数 (setup_logger)，用于创建和配置
具有文件轮转功能的 logger 实例。

日志默认输出到 'logs' 目录下的:
    - all.log: 记录所有级别的日志
    - error.log: 仅记录 ERROR 及以上级别的日志

作者：竹子是不秋草
创建日期：2026-02-04
最后修改：2026-03-24
依赖：logging, pathlib
"""

import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional

# --- 常量配置 ---
DEFAULT_LOG_DIR = Path("logs")
DEFAULT_ALL_LOG_FILE = "all.log"
DEFAULT_ERROR_LOG_FILE = "error.log"
MAX_BYTES = 10 * 1024 * 1024  # 10 MB
BACKUP_COUNT = 5
ENCODING = "utf-8"

# 日志格式配置
LOG_FORMAT = (
    "%(asctime)s - %(levelname)s - %(filename)s:%(funcName)s:%(lineno)d - %(message)s"
)
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def setup_logger(
    name: str = __name__,
    level: int = logging.DEBUG,
    console: bool = False,
    log_dir: Optional[Path] = None,
) -> logging.Logger:
    """
    创建并配置一个带有文件轮转功能的 Logger。

    该函数会尝试添加文件处理器。如果由于权限或路径问题无法创建文件，
    函数将记录警告并仅保留控制台输出（如果启用），而不会抛出异常中断程序。

    Args:
        name (str): Logger 的名称，通常为 __name__。
        level (int): 日志级别，默认为 logging.DEBUG。
        console (bool): 是否同时输出到控制台，默认为 False。
        log_dir (Optional[Path]): 日志目录路径，默认为当前目录下的 'logs'。

    Returns:
        logging.Logger: 配置好的 Logger 实例。
    """
    # 确定日志目录
    target_log_dir = log_dir if log_dir else DEFAULT_LOG_DIR
    
    # 获取或创建 logger
    logger = logging.getLogger(name)
    
    # 防止重复添加 Handler (关键优化)
    # 如果 logger 已经有 handler 且不是由本函数逻辑控制的（或者为了避免多次调用本函数重复添加）
    # 这里我们检查是否已经添加过我们特定类型的 handler，或者简单检查 handlers 列表
    # 最佳实践：如果是应用启动时的统一配置，通常假设只调用一次。
    # 但为了安全，如果 handlers 已存在，我们不再重复添加，除非显式需要重置。
    if logger.handlers:
        # 如果已经配置过，直接返回，避免重复打印
        # 注意：如果需要动态更改级别，可以在这里调整 logger.setLevel(level)
        return logger

    logger.setLevel(level)
    logger.propagate = False  # 防止日志传播到父级 logger 造成重复

    # 创建格式化器
    formatter = logging.Formatter(LOG_FORMAT, DATE_FORMAT)

    # --- 文件处理器配置 ---
    try:
        # 确保目录存在
        target_log_dir.mkdir(parents=True, exist_ok=True)

        # 1. 全量日志处理器 (all.log)
        all_log_path = target_log_dir / DEFAULT_ALL_LOG_FILE
        all_file_handler = RotatingFileHandler(
            all_log_path,
            maxBytes=MAX_BYTES,
            backupCount=BACKUP_COUNT,
            encoding=ENCODING,
            delay=True,  # 延迟打开文件，直到第一条日志写入，提高启动速度
        )
        all_file_handler.setLevel(level)
        all_file_handler.setFormatter(formatter)
        logger.addHandler(all_file_handler)

        # 2. 错误日志处理器 (error.log)
        error_log_path = target_log_dir / DEFAULT_ERROR_LOG_FILE
        error_file_handler = RotatingFileHandler(
            error_log_path,
            maxBytes=MAX_BYTES,
            backupCount=BACKUP_COUNT,
            encoding=ENCODING,
            delay=True,
        )
        error_file_handler.setLevel(logging.ERROR)
        error_file_handler.setFormatter(formatter)
        logger.addHandler(error_file_handler)

    except (OSError, PermissionError) as e:
        # 如果文件写入失败（如权限不足），不崩溃程序，而是通过临时控制台输出警告
        print(f"Warning: Failed to initialize file logging for '{name}': {e}", file=sys.stderr)
        # 即使文件失败，如果用户请求了 console，下面依然会添加 console handler

    # --- 控制台处理器配置 ---
    if console:
        console_handler = logging.StreamHandler(sys.stderr)  # 最佳实践：日志输出到 stderr
        console_handler.setLevel(level)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    return logger


if __name__ == "__main__":
    # 示例用法
    # 启用 console=True 以便在运行时看到输出
    logger = setup_logger(name=__name__, console=True)
    
    logger.debug("DEBUG 测试消息")
    logger.info("INFO 测试消息")
    logger.warning("WARNING 测试消息")
    logger.error("ERROR 测试消息")
    logger.critical("CRITICAL 测试消息")
    
    # 测试重复调用（应该不会重复输出）
    logger2 = setup_logger(name=__name__, console=True)
    logger2.info("这是一条重复调用后的测试消息（应只出现一次）")