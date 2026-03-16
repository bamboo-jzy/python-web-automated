"""
文件数据读取器模块

该模块提供了一个统一的接口来安全地读取多种格式的数据文件（JSON, CSV, Excel, TOML）。
核心功能包括路径安全验证、统一的日志记录、详细的错误处理以及数据摘要生成。

作者：资深Python开发工程师
创建日期：2026-02-04
依赖：tomllib, pathlib, typing, pandas, common.log_config
"""

from pathlib import Path
from typing import Any, Dict, Tuple, Union

import pandas as pd
import tomllib

from common.log_config import setup_logger

logger = setup_logger()

SUPPORTED_EXTENSIONS = {".json", ".csv", ".xlsx", ".xls", ".toml"}
DEFAULT_ENCODING = "utf-8"


class FileDataReader:
    """安全、健壮的多格式文件读取器，支持结构化日志与精确错误处理

    该类负责处理文件路径的验证、规范化，并根据文件扩展名调用相应的读取器。
    所有读取操作都会伴随详细的日志记录。

    属性:
        _absolute_file_path (Path): 规范化的绝对文件路径
        _file_ext (str): 小写的文件扩展名
    """

    def __init__(self, file_path: Union[str, Path]) -> None:
        """初始化文件数据读取器

        Args:
            file_path (Union[str, Path]): 文件路径，可以是字符串或Path对象

        Raises:
            FileNotFoundError: 当文件不存在时
            ValueError: 当路径不是文件或格式不支持时
            PermissionError: 当尝试访问项目根目录外的路径时
        """
        path_obj = Path(file_path) if isinstance(file_path, str) else file_path
        project_root = Path(__file__).resolve().parent.parent

        if path_obj.is_absolute():
            self._absolute_file_path = path_obj.resolve(strict=False)
        else:
            self._absolute_file_path = (project_root / path_obj).resolve(strict=False)

        try:
            self._absolute_file_path.relative_to(project_root)
        except ValueError:
            error_msg = f"拒绝访问项目根目录外的路径: {self._absolute_file_path}"
            logger.error(error_msg)
            raise PermissionError(error_msg)

        if not self._absolute_file_path.exists():
            error_msg = f"文件不存在: {self._absolute_file_path}"
            logger.error(error_msg)
            raise FileNotFoundError(error_msg)

        if not self._absolute_file_path.is_file():
            error_msg = f"路径非文件: {self._absolute_file_path}"
            logger.error(error_msg)
            raise ValueError(error_msg)

        self._file_ext = self._absolute_file_path.suffix.lower()
        if self._file_ext not in SUPPORTED_EXTENSIONS:
            supported_str = ", ".join(sorted(SUPPORTED_EXTENSIONS))
            error_msg = (
                f"不支持的文件格式 '{self._file_ext}' | "
                f"路径: {self._absolute_file_path} | "
                f"支持格式: {supported_str}"
            )
            logger.error(error_msg)
            raise ValueError(error_msg)

        logger.debug(
            f"初始化文件读取器: {self._absolute_file_path.name} (格式: {self._file_ext})"
        )

    @property
    def file_path(self) -> Path:
        """规范化后的绝对文件路径"""
        return self._absolute_file_path

    @property
    def file_extension(self) -> str:
        """小写文件扩展名（含点）"""
        return self._file_ext

    def _summarize_data(self, data: Any) -> str:
        """生成安全的数据摘要（避免日志爆炸）

        为读取的数据生成简要描述，用于日志记录。

        Args:
            data (Any): 待摘要的数据对象

        Returns:
            str: 数据摘要字符串
        """
        if isinstance(data, pd.DataFrame):
            if data.empty:
                return "空数据集"
            cols = list(data.columns[:3])
            col_repr = f"{cols}{'...' if len(data.columns) > 3 else ''}"
            return f"形状{data.shape} | 列预览: {col_repr} (共{len(data.columns)}列)"
        elif isinstance(data, dict):
            if not data:
                return "空字典"
            keys = list(data.keys())[:3]
            key_repr = f"{keys}{'...' if len(data) > 3 else ''}"
            return f"键数量: {len(data)} | 预览: {key_repr}"
        return f"类型: {type(data).__name__}"

    def _read_toml(self, **kwargs) -> dict[str, Any]:
        """读取TOML文件

        Args:
            **kwargs: 传递给tomllib.load的额外参数

        Returns:
            dict[str, Any]: 解析后的字典数据

        Raises:
            ValueError: 当解析失败或IO错误时
        """
        logger.debug(f"读取 TOML: {self._absolute_file_path.name}")
        try:
            with open(self._absolute_file_path, "rb") as f:
                data = tomllib.load(f, **kwargs)
            logger.debug(f"TOML 读取成功 | {self._summarize_data(data)}")
            return data
        except tomllib.TOMLDecodeError as e:
            error_msg = f"TOML 文件 '{self._absolute_file_path.name}' 解析失败: {str(e)[:200]}"
            logger.error(error_msg)
            raise ValueError(error_msg) from e
        except (OSError, IOError) as e:
            error_msg = f"TOML 文件 '{self._absolute_file_path.name}' IO错误: {e}"
            logger.error(error_msg)
            raise ValueError(error_msg) from e

    def _read_csv(self, **kwargs) -> pd.DataFrame:
        """读取CSV文件

        Args:
            **kwargs: 传递给pandas.read_csv的额外参数

        Returns:
            pd.DataFrame: 解析后的DataFrame

        Raises:
            ValueError: 当解析失败或IO错误时
        """
        logger.debug(f"读取 CSV: {self._absolute_file_path.name}")
        try:
            df = pd.read_csv(self._absolute_file_path, **kwargs)
        except pd.errors.EmptyDataError:
            logger.warning(f"CSV 文件为空: {self._absolute_file_path.name}")
            df = pd.DataFrame()
        except (pd.errors.ParserError, OSError, IOError, ValueError) as e:
            error_msg = f"CSV 文件 '{self._absolute_file_path.name}' 读取/解析错误: {str(e)[:200]}"
            logger.error(error_msg)
            raise ValueError(error_msg) from e

        logger.debug(f"CSV 读取成功 | {self._summarize_data(df)}")
        return df

    def _read_excel(self, **kwargs) -> pd.DataFrame:
        """读取Excel文件

        Args:
            **kwargs: 传递给pandas.read_excel的额外参数

        Returns:
            pd.DataFrame: 解析后的DataFrame

        Raises:
            ValueError: 当解析失败或IO错误时
        """
        logger.debug(f"读取 Excel: {self._absolute_file_path.name}")
        try:
            df = pd.read_excel(self._absolute_file_path, **kwargs)
        except (ValueError, OSError, IOError) as e:
            error_msg = f"Excel 文件 '{self._absolute_file_path.name}' 读取/解析错误: {str(e)[:200]}"
            logger.error(error_msg)
            raise ValueError(error_msg) from e

        logger.debug(f"Excel 读取成功 | {self._summarize_data(df)}")
        return df

    def _read_json(self, **kwargs) -> pd.DataFrame:
        """读取JSON文件

        Args:
            **kwargs: 传递给pandas.read_json的额外参数

        Returns:
            pd.DataFrame: 解析后的DataFrame

        Raises:
            ValueError: 当解析失败或IO错误时
        """
        logger.debug(f"→ 读取 JSON: {self._absolute_file_path.name}")
        try:
            df = pd.read_json(self._absolute_file_path, **kwargs)
        except (ValueError, OSError, IOError) as e:
            error_msg = f"JSON 文件 '{self._absolute_file_path.name}' 读取/解析错误: {str(e)[:200]}"
            logger.error(error_msg)
            raise ValueError(error_msg) from e

        logger.debug(f"JSON 读取成功 | {self._summarize_data(df)}")
        return df

    def read(self, **kwargs) -> Tuple[str, Union[pd.DataFrame, Dict[str, Any]]]:
        """统一读取接口

        根据文件扩展名自动分发到相应的读取方法。

        Args:
            **kwargs: 传递给底层读取器的额外参数

        Returns:
            Tuple[str, Union[pd.DataFrame, dict]]:
                包含文件扩展名和解析后数据的元组。
                TOML文件返回字典，其他格式返回DataFrame。

        Raises:
            ValueError: 文件内容解析失败
            OSError/IOError: 文件系统级错误
            NotImplementedError: 未实现的文件读取器
        """
        logger.info(f"开始读取 [{self._file_ext}] {self._absolute_file_path.name}")

        # 映射扩展名到读取方法
        reader_map = {
            ".json": self._read_json,
            ".csv": self._read_csv,
            ".toml": self._read_toml,
            ".xls": self._read_excel,
            ".xlsx": self._read_excel,
        }

        reader_func = reader_map.get(self._file_ext)
        if not reader_func:
            error_msg = f"未实现的文件读取器: {self._file_ext}"
            logger.error(error_msg)
            raise NotImplementedError(error_msg)

        try:
            data = reader_func(**kwargs)
            logger.debug(
                f"读取完成 [{self._file_ext}] {self._absolute_file_path.name}"
            )
            return self._file_ext, data
        except Exception as e:
            logger.error(
                f"读取失败 [{self._file_ext}] {self._absolute_file_path.name}: {e}"
            )
            raise


if __name__ == "__main__":
    reader = FileDataReader("config/database.toml")
    file_ext, res = reader.read()
    print(res)
