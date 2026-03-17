import allure
import pytest

def pytest_runtest_makereport(item: pytest.Item, call: pytest.CallInfo) -> None:
    """测试失败时自动截图

    在测试用例生命周期且失败时，自动截图并自动上传附件到 allure 报告

    :param item: pytest.Item 对象
    :param call: pytest.CallInfo 对象
    :return:  None
    """
    """
    
    在测试用例生命周期且失败时触发
    截图目录：项目根目录下的'screenshots'文件夹（需提前创建）
    """

    if (call.when == "call" and call.excinfo) or (call.when == "setup" and call.excinfo) or (call.when == "teardown" and call.excinfo):
        # 检查测试用例是否使用了page fixture
        if hasattr(item, "funcargs") and "page" in item.funcargs:
            page = item.funcargs["page"]

            # 截图并保存
            try:
                screenshot_bytes = page.screenshot()
                allure.attach(
                    screenshot_bytes,
                    name="screenshot",
                    attachment_type=allure.attachment_type.PNG
                )
            except Exception as e:
                # 处理截图失败的情况
                error_msg = f"截图失败: {str(e)}"
                allure.attach(
                    error_msg.encode(),
                    name="screenshot_error",
                    attachment_type=allure.attachment_type.TEXT
                )