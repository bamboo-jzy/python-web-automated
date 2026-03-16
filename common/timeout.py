# -*- coding: utf-8 -*-
"""
超时装饰器模块

模块功能：
    提供一个可复用的超时装饰器，为目标函数添加执行时间限制，超时后抛出AssertionError并记录错误日志。
适用场景：
    1. 测试用例执行超时控制，防止单个用例阻塞整体测试流程；
    2. 外部接口调用、耗时操作的执行时间限制；
    3. 需要严格控制执行时长的函数调用场景。
"""

import concurrent.futures
import functools
from typing import Any, Callable

from common.log_config import setup_logger

logger = setup_logger()


def timeout(seconds: int = 10) -> Callable:
    """
    超时装饰器：为函数添加执行时间限制，超时则抛出AssertionError并记录日志。

    装饰器内部通过ThreadPoolExecutor实现超时控制，executor实例作为函数属性单例存在，
    避免重复创建线程池资源。

    Args:
        seconds (int): 超时时间（单位：秒），非负整数，默认为10秒。
    
    Returns:
        Callable: 包装后的目标函数，保留原函数的元信息（名称、文档等）。
    
    Raises:
        AssertionError: 当被装饰函数执行时间超过指定seconds时抛出，包含超时提示信息。
    
    注意：
        1. 装饰器内部的executor为单例ThreadPoolExecutor，max_workers=1，高并发场景下可能存在执行排队；
        2. 超时异常会屏蔽原TimeoutError，通过`from None`切断异常链，仅暴露AssertionError；
        3. 未校验seconds参数的合法性（如负数），传入负数会导致future.result()抛出ValueError。
    """
    if not hasattr(timeout, 'executor'):
        setattr(timeout, 'executor', concurrent.futures.ThreadPoolExecutor(max_workers=1))
    
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            """
            装饰器内部包装函数，执行目标函数并监控超时。

            Args:
                *args (Any): 目标函数的位置参数。
                **kwargs (Any): 目标函数的关键字参数。
            
            Returns:
                Any: 目标函数的执行结果。
            
            Raises:
                AssertionError: 函数执行超时时抛出。
            """
            executor = getattr(timeout, 'executor')
            future = executor.submit(func, *args, **kwargs)
            try:
                return future.result(timeout=seconds)
            except concurrent.futures.TimeoutError:
                error_msg = f"测试用例 {func.__name__} 已执行超过 {seconds} 秒。"
                logger.error(error_msg)
                raise AssertionError(error_msg) from None

        return wrapper

    return decorator