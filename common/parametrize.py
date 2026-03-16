# -*- coding: utf-8 -*-
"""
pytest参数化数据加载模块

模块功能：提供基于文件的pytest参数化装饰器工具，支持从外部文件读取测试数据，
          自动转换为pytest.mark.parametrize所需格式，并支持为测试用例添加自定义pytest标记。
适用场景：自动化测试场景中，需从Excel/CSV等文件批量加载测试数据，
          并为不同测试用例配置自定义pytest标记（如skip、xfail等）的场景。
"""
from typing import Callable, Tuple, Dict, List, cast

import pytest
from pandas import DataFrame

from common.file_data_reader import FileDataReader
from common.log_config import setup_logger

logger = setup_logger()


def _dataframe_to_parametrize_data(df: DataFrame) -> Tuple[str, List[tuple], Dict[int, List[str]]]:
    """
    将包含测试数据的DataFrame转换为pytest.mark.parametrize可直接使用的数据格式

    处理逻辑：提取DataFrame中的参数列名、参数数据，解析'mark'列生成标记映射（标记间用短横线分隔）。

    Args:
        df (DataFrame): 包含测试数据的DataFrame对象，可选包含'mark'列（用于定义pytest标记）。

    Returns:
        Tuple[str, List[tuple], Dict[int, List[str]]]: 元组包含三个核心元素：
            - str: 以逗号分隔的参数名称字符串（如"username,password"）；
            - List[tuple]: 参数化数据的元组列表，每个元组对应一行测试数据；
            - Dict[int, List[str]]: 行索引到标记列表的映射字典（无标记则为空字典）。

    注意:
        1. DataFrame中的空值会被替换为空字符串，避免参数化时出现None值；
        2. 若DataFrame为空，返回的参数化数据列表也为空，需调用方自行校验。
    """
    mark_col = df.get("mark")
    mark_data = {}
    if mark_col is not None:
        mark_series = mark_col.fillna('').apply(lambda x: [s for s in str(x).split('-') if s])
        mark_data = mark_series.to_dict()

    non_mark_df = df.drop(columns=["mark"], errors="ignore").fillna('')
    parameterized_variables = ",".join(non_mark_df.columns)
    parameterized_data = [tuple(row) for row in non_mark_df.values.tolist()]

    return parameterized_variables, parameterized_data, mark_data


def parametrize(file_path: str, **kwargs) -> Callable[[Callable], Callable]:
    """
    从指定文件加载测试数据，生成pytest参数化装饰器（支持自定义标记）

    核心流程：读取文件数据→转换为DataFrame→解析参数和标记→生成带标记的parametrize装饰器。

    Args:
        file_path (str): 测试数据文件路径（支持FileDataReader可读取的类型：Excel/CSV等）；
        **kwargs: 传递给FileDataReader.read()的额外参数（如sheet_name、encoding、sep等）。

    Returns:
        Callable[[Callable], Callable]: pytest的参数化装饰器函数，可直接装饰测试函数。

    Raises:
        ValueError: 当文件中无有效测试数据时抛出；
        FileNotFoundError: 当指定的file_path文件不存在时（由FileDataReader触发）；
        IOError: 当文件读取失败时（由FileDataReader触发）。

    注意:
        1. 若mark列中的标记名称不存在于pytest.mark中（如自定义标记未注册），
           该标记会被忽略并记录警告日志；
        2. 数据文件中的空值会被统一替换为空字符串，避免测试函数接收None值导致异常。
    """
    reader = FileDataReader(file_path)
    _, data_frame = reader.read(**kwargs)

    variables, data, marks_map = _dataframe_to_parametrize_data(cast(DataFrame, data_frame))

    if not data:
        error_msg = f"{file_path} 文件中，无测试数据"
        logger.error(error_msg)
        raise ValueError(error_msg)

    def _apply_marks_to_data(
        data_list: List[tuple],
        marks_mapping: Dict[int, List[str]]
    ) -> List:
        """
        为参数化数据列表中的测试用例项绑定对应的pytest标记

        Args:
            data_list (List[tuple]): 原始参数化数据元组列表；
            marks_mapping (Dict[int, List[str]]): 行索引到标记列表的映射字典。

        Returns:
            List[pytest.param]: 包含pytest标记的参数化数据列表，每个元素为pytest.param对象。

        注意:
            若marks_mapping为空，会为所有测试用例生成无标记的pytest.param对象。
        """
        if not marks_mapping:
            logger.debug("没有找到 'mark' 列或该列为空，跳过标记处理。")
            return [pytest.param(*item) for item in data_list]

        marked_params = []
        for index, item in enumerate(data_list):
            current_marks = []
            if index in marks_mapping:
                for mark_name in marks_mapping[index]:
                    if hasattr(pytest.mark, mark_name):
                        current_marks.append(getattr(pytest.mark, mark_name))
                    else:
                        logger.warning(f"标记 '{mark_name}' 不存在，将被跳过。")
            marked_params.append(pytest.param(*item, marks=current_marks))
        
        return marked_params

    final_data = _apply_marks_to_data(data, marks_map)
    return pytest.mark.parametrize(variables, final_data)
    
if __name__ == "__main__":
    pass