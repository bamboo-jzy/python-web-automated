"""
日志处理器模块

该模块提供了一个标准化的日志配置函数 (setup_logger)，用于创建和配置
具有文件轮转功能的 logger 实例。日志默认输出到 'logs' 目录下的
 all.log (所有级别) 和 error.log (仅错误级别) 两个文件中。

作者：资深Python开发工程师
创建日期：2026-02-04
依赖：logging, logging.handlers, os
"""

import logging
from logging.handlers import RotatingFileHandler
import os

# 定义日志输出格式：包含时间、日志等级、文件名/函数名/行号及消息内容
LOG_FORMAT = "%(asctime)s - %(levelname)s - %(filename)s:%(funcName)s:%(lineno)d - %(message)s"
# 定义时间显示格式
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def setup_logger(name: str = __name__, level: int = logging.DEBUG) -> logging.Logger:
    """配置并返回一个具有文件轮转功能的logger实例

    该函数会创建一个logger，并添加两个轮转文件处理器：
    1. all_handler: 记录所有级别(DEBUG及以上)的日志到 all.log
    2. error_handler: 仅记录错误级别(ERROR及以上)的日志到 error.log

    Args:
        name (str, optional): logger的名称，默认为当前模块名
        level (int, optional): 日志记录级别，默认为 DEBUG

    Returns:
        logging.Logger: 配置好的logger实例
    """
    logs_dir = "logs"
    if not os.path.exists(logs_dir):
        os.makedirs(logs_dir, exist_ok=True)
    
    formatter = logging.Formatter(LOG_FORMAT, DATE_FORMAT)
    
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.propagate = False
    
    all_handler = RotatingFileHandler(
        os.path.join(logs_dir, "all.log"),
        maxBytes=10 * 1024 * 1024,
        backupCount=5, 
        encoding="utf-8"
    )
    all_handler.setLevel(level)
    all_handler.setFormatter(formatter)
    logger.addHandler(all_handler)
    
    error_handler = RotatingFileHandler(
        os.path.join(logs_dir, "error.log"),
        maxBytes=10 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8"
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(formatter)
    logger.addHandler(error_handler)
    
    return logger


if __name__ == "__main__":
    logger = setup_logger()
    logger.debug("DEBUG 测试")
    logger.info("INFO 测试")
    logger.warning("WARNING 测试")
    logger.error("ERROR 测试")
    logger.critical("CRITICAL 测试")