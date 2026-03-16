import argparse
import sys

import pytest

from common.log_config import setup_logger

logger = setup_logger()


# 构建参数解析器，传递 pytest 参数，后续根据需求创建自定义参数
def parse_arguments() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="pytest 启动脚本（支持自定义参数 + pytest 参数）",
        add_help=False,  # 隐藏默认帮助信息，避免与 pytest 的 -h 冲突
    )

    parser.add_argument(
        "-h",
        "--help",
        action="store_true",
        help="显示帮助信息（包含自定义参数和pytest原生参数）",
    )

    return parser


# 运行测试
def run_tests(parser: argparse.ArgumentParser) -> int:
    """运行测试"""
    # 解析自定义参数和 pytest 参数
    customize_args, pytest_args = parser.parse_known_args()

    # 如果用户请求帮助，则打印帮助信息并退出
    if customize_args.help:
        parser.print_help()
        print("\n=== pytest 原生参数 help 信息 ===")
        pytest.main(["-h"])
        return 0

    # 开始运行测试
    try:
        logger.info(f"开始运行测试，，pytest参数: {pytest_args}")
        exit_code = pytest.main(pytest_args)

        exit_code_value = (
            exit_code.value if isinstance(exit_code, pytest.ExitCode) else exit_code
        )
        exit_messages = {
            0: "✅ 全部测试用例通过",
            1: "⚠️ 部分测试用例未通过",
            2: "❌ 测试过程中有中断或其他非正常终止",
            3: "❌ 内部错误",
            4: "❌ 命令行参数错误",
            5: "❌ 没有收集到任何测试用例",
        }

        logger.info(
            exit_messages.get(exit_code_value, f"❓ 未知的退出码: {exit_code_value}")
        )
        return exit_code_value

    except Exception:
        logger.exception("运行测试时发生致命错误:")
        return 1


def main():
    parser = parse_arguments()
    exit_code = run_tests(parser)
    sys.exit(exit_code)
    
if __name__ == "__main__":
    main()
