import allure
import pytest

SCREENSHOT_NAME = "Failure_Screenshot"
SCREENSHOT_TYPE = allure.attachment_type.PNG


def pytest_runtest_makereport(item: pytest.Item, call: pytest.CallInfo) -> None:
    """
    测试失败时自动截图并附加到 Allure 报告。

    在测试用例的 setup、call 或 teardown 任一阶段失败时，
    自动截取 Playwright page 对象的画面并将其作为附件添加到 Allure 测试报告中。

    Args:
        item (pytest.Item): 当前执行的测试项对象。
        call (pytest.CallInfo): 包含测试调用结果和异常信息的对象。
    """

    is_test_failed = call.excinfo is not None
    is_critical_phase = call.when in ("setup", "call", "teardown")
    
    if is_critical_phase and is_test_failed:
        
        page = getattr(item, "funcargs", {}).get("page")

        if page is not None:
            try:
                screenshot_bytes = page.screenshot()
                
                allure.attach(
                    screenshot_bytes,
                    name=f"{SCREENSHOT_NAME}_{item.name}",
                    attachment_type=SCREENSHOT_TYPE
                )
            except Exception as e:
                error_message = f"在测试 '{item.name}' 失败后尝试截图时发生错误: {repr(e)}"
                allure.attach(
                    body=error_message,
                    name="Screenshot_Error_Message",
                    attachment_type=allure.attachment_type.TEXT
                )
        else:
            no_page_msg = (
                f"测试 '{item.name}' 失败，但在其 fixtures 中未找到 'page' 对象。"
                f"无法生成自动截图。"
            )
            allure.attach(
                body=no_page_msg,
                name="No_Page_Fixture_Available",
                attachment_type=allure.attachment_type.TEXT
            )