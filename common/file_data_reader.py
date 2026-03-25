"""
文件数据读取器模块

提供一个统一、安全且健壮的接口来读取多种格式的数据文件（JSON, CSV, Excel, TOML）。
核心功能包括：
    - 严格的路径安全验证（防止目录遍历攻击）
    - 统一的日志记录与错误处理
    - 智能的数据摘要生成
    - 类型安全的返回结果

作者：资深Python开发工程师
创建日期：2026-02-04
最后修改：2026-03-24
依赖：tomllib, pathlib, typing, pandas, common.log_config
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Dict, Final, Optional, Tuple, Union

import pandas as pd
import tomllib

from common.log_config import setup_logger

logger = setup_logger(name=__name__)

# --- 常量配置 ---
SUPPORTED_EXTENSIONS: Final[set[str]] = {".json", ".csv", ".xlsx", ".xls", ".toml"}
DEFAULT_ENCODING: Final[str] = "utf-8"
MAX_LOG_PREVIEW_ITEMS: Final[int] = 5


class FileDataReaderError(Exception):
    """自定义基础异常类，用于文件读取相关的错误"""

    pass


class FileDataReader:
    """
    安全、健壮的多格式文件读取器。

    负责处理文件路径的验证、规范化，并根据文件扩展名调用相应的读取器。
    所有操作均包含详细的安全检查和日志记录。

    Attributes:
        file_path (Path): 经过验证和规范的绝对文件路径。
        file_extension (str): 小写的文件扩展名。
    """

    __slots__ = ("_absolute_file_path", "_file_ext")

    def __init__(
        self, file_path: Union[str, Path], root_path: Optional[Path] = None
    ) -> None:
        """
        初始化文件数据读取器。

        Args:
            file_path: 文件路径（相对或绝对）。
            root_path: 项目的根目录路径，用于安全限制。
                       如果为 None，默认为当前工作目录。
                       绝对路径文件必须在 root_path 下才允许访问。

        Raises:
            FileNotFoundError: 文件不存在。
            ValueError: 路径不是文件或格式不支持。
            PermissionError: 路径超出允许的根目录范围（防止目录遍历）。
        """
        path_obj = Path(file_path) if isinstance(file_path, str) else file_path
        base_root = root_path if root_path else Path.cwd()

        try:
            resolved_root = base_root.resolve(strict=True)
        except FileNotFoundError:
            raise ValueError(f"指定的根目录不存在: {base_root}")

        if path_obj.is_absolute():
            resolved_path = path_obj.resolve(strict=False)
        else:
            resolved_path = (resolved_root / path_obj).resolve(strict=False)

        try:
            resolved_path.relative_to(resolved_root)
        except ValueError:
            msg = f"拒绝访问：路径 '{resolved_path}' 超出根目录 '{resolved_root}' 范围"
            logger.error(msg)
            raise PermissionError(msg)

        # 存在性与类型检查
        if not resolved_path.exists():
            msg = f"文件不存在: {resolved_path}"
            logger.error(msg)
            raise FileNotFoundError(msg)

        if not resolved_path.is_file():
            msg = f"路径指向的不是文件: {resolved_path}"
            logger.error(msg)
            raise ValueError(msg)

        # 扩展名检查
        file_ext = resolved_path.suffix.lower()
        if file_ext not in SUPPORTED_EXTENSIONS:
            supported_list = ", ".join(sorted(SUPPORTED_EXTENSIONS))
            msg = f"不支持的文件格式 '{file_ext}'. 支持格式: [{supported_list}]"
            logger.error(msg)
            raise ValueError(msg)

        # 赋值
        self._absolute_file_path = resolved_path
        self._file_ext = file_ext

        logger.debug(
            f"读取器初始化成功: {self._absolute_file_path.name} "
            f"(类型: {self._file_ext}, 大小: {self._absolute_file_path.stat().st_size} bytes)"
        )

    @property
    def file_path(self) -> Path:
        """规范化后的绝对文件路径"""
        return self._absolute_file_path

    @property
    def file_extension(self) -> str:
        """小写文件扩展名（含点）"""
        return self._file_ext

    def _summarize_data(self, data: Union[pd.DataFrame, Dict[str, Any]]) -> str:
        """
        生成安全的数据摘要，防止日志爆炸。

        Args:
            data: 读取到的数据对象。

        Returns:
            简短的数据描述字符串。
        """
        if isinstance(data, pd.DataFrame):
            if data.empty:
                return "空 DataFrame"

            rows, cols = data.shape
            col_sample = list(data.columns)[:MAX_LOG_PREVIEW_ITEMS]
            suffix = "..." if cols > MAX_LOG_PREVIEW_ITEMS else ""
            return f"DataFrame[{rows}x{cols}] | 列: {col_sample}{suffix}"

        if isinstance(data, dict):
            if not data:
                return "空字典"
            keys_sample = list(data.keys())[:MAX_LOG_PREVIEW_ITEMS]
            suffix = "..." if len(data) > MAX_LOG_PREVIEW_ITEMS else ""
            return f"Dict[keys={len(data)}] | 预览: {keys_sample}{suffix}"

        return f"UnknownType[{type(data).__name__}]"

    def _read_toml(self, **kwargs: Any) -> Dict[str, Any]:
        """读取 TOML 文件"""
        logger.debug(f"解析 TOML: {self._absolute_file_path.name}")
        try:
            with open(self._absolute_file_path, "rb") as f:
                data = tomllib.load(f, **kwargs)

            if not isinstance(data, dict):
                logger.warning("TOML 根节点不是字典，包裹为字典")
                data = {"root": data}

            logger.debug(f"TOML 读取成功: {self._summarize_data(data)}")
            return data
        except tomllib.TOMLDecodeError as e:
            msg = f"TOML 语法错误 ({self._absolute_file_path.name}): {str(e)[:150]}"
            logger.error(msg)
            raise FileDataReaderError(msg) from e
        except OSError as e:
            msg = f"TOML 文件 IO 错误: {e}"
            logger.error(msg)
            raise FileDataReaderError(msg) from e

    def _read_csv(self, **kwargs: Any) -> pd.DataFrame:
        """读取 CSV 文件"""
        logger.debug(f"解析 CSV: {self._absolute_file_path.name}")
        if "encoding" not in kwargs:
            kwargs["encoding"] = DEFAULT_ENCODING

        try:
            df = pd.read_csv(self._absolute_file_path, **kwargs)
        except pd.errors.EmptyDataError:
            logger.warning(
                f"CSV 文件为空，返回空 DataFrame: {self._absolute_file_path.name}"
            )
            return pd.DataFrame()
        except UnicodeDecodeError as e:
            msg = f"CSV 编码错误 ({DEFAULT_ENCODING}): {e}"
            logger.error(msg)
            raise FileDataReaderError(msg) from e
        except pd.errors.ParserError as e:
            msg = f"CSV 解析失败: {str(e)[:150]}"
            logger.error(msg)
            raise FileDataReaderError(msg) from e
        except Exception as e:
            msg = f"CSV 读取未知错误: {e}"
            logger.error(msg)
            raise FileDataReaderError(msg) from e

        logger.debug(f"CSV 读取成功: {self._summarize_data(df)}")
        return df

    def _read_excel(self, **kwargs: Any) -> pd.DataFrame:
        """读取 Excel 文件 (.xlsx, .xls)"""
        logger.debug(f"解析 Excel: {self._absolute_file_path.name}")
        try:
            df = pd.read_excel(self._absolute_file_path, **kwargs)
        except ValueError as e:
            if "missing" in str(e).lower() or "engine" in str(e).lower():
                msg = f"Excel 读取失败，可能缺少依赖库 (openpyxl/xlrd): {e}"
            else:
                msg = f"Excel 解析错误: {str(e)[:150]}"
            logger.error(msg)
            raise FileDataReaderError(msg) from e
        except Exception as e:
            msg = f"Excel 读取未知错误: {e}"
            logger.error(msg)
            raise FileDataReaderError(msg) from e

        logger.debug(f"Excel 读取成功: {self._summarize_data(df)}")
        return df

    def _read_json(self, **kwargs: Any) -> pd.DataFrame:
        """
        读取 JSON 文件并转换为 DataFrame。

        Note: 如果 JSON 结构非常复杂且不适合表格化，建议直接使用 json 模块。
              此方法默认假设 JSON 是记录列表或适合表格化的结构。
        """
        logger.debug(f"解析 JSON: {self._absolute_file_path.name}")

        if "orient" not in kwargs:
            kwargs["orient"] = "records"

        try:
            df = pd.read_json(self._absolute_file_path, **kwargs)
        except ValueError as e:
            msg = f"JSON 转换为 DataFrame 失败 (结构可能不兼容): {str(e)[:150]}"
            logger.error(msg)
            raise FileDataReaderError(msg) from e
        except Exception as e:
            msg = f"JSON 读取未知错误: {e}"
            logger.error(msg)
            raise FileDataReaderError(msg) from e

        logger.debug(f"JSON 读取成功: {self._summarize_data(df)}")
        return df

    def read(self, **kwargs: Any) -> Tuple[str, Union[pd.DataFrame, Dict[str, Any]]]:
        """
        统一读取接口。

        根据文件扩展名自动分发到对应的读取方法。

        Args:
            **kwargs: 传递给底层读取函数 (pd.read_csv, tomllib.load 等) 的参数。

        Returns:
            Tuple[str, Union[pd.DataFrame, Dict[str, Any]]]: (文件扩展名, 数据对象)。
                - CSV, Excel, JSON -> pd.DataFrame
                - TOML -> Dict[str, Any]

        Raises:
            FileDataReaderError: 读取或解析过程中发生的错误。
            NotImplementedError: 支持列表中但未实现读取逻辑的扩展名。
        """
        logger.info(f"开始读取文件: {self._absolute_file_path.name} [{self._file_ext}]")

        try:
            match self._file_ext:
                case ".toml":
                    data = self._read_toml(**kwargs)
                case ".csv":
                    data = self._read_csv(**kwargs)
                case ".json":
                    data = self._read_json(**kwargs)
                case ".xlsx" | ".xls":
                    data = self._read_excel(**kwargs)
                case _:
                    msg = f"未实现的读取器逻辑: {self._file_ext}"
                    logger.critical(msg)
                    raise NotImplementedError(msg)

            logger.info(f"文件读取完成: {self._absolute_file_path.name}")
            return self._file_ext, data

        except FileDataReaderError:
            raise
        except Exception as e:
            msg = f"读取文件时发生未预期错误: {e}"
            logger.exception(msg)
            raise FileDataReaderError(msg) from e


if __name__ == "__main__":
    # 示例用法
    # 假设项目结构允许访问 config/database.toml
    test_file = "config/database.toml"

    # 创建临时测试文件以便演示 (实际使用时不需要)
    Path("config").mkdir(exist_ok=True)
    if not Path(test_file).exists():
        Path(test_file).write_text(
            'title = "Test DB"\n[db]\nhost = "localhost"\nport = 5432'
        )

    try:
        reader = FileDataReader(test_file)
        ext, data = reader.read()

        print(f"--- 读取成功 ({ext}) ---")
        if isinstance(data, pd.DataFrame):
            print(data.head())
        else:
            import json

            print(json.dumps(data, indent=2, ensure_ascii=False))

    except Exception as e:
        print(f"程序执行失败: {e}")
        sys.exit(1)
