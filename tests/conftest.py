"""pytest 路径配置与插件注册。"""

import shutil
import sys
import uuid
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# v4.0: 注册 asyncio 标记（Task 4 model_client 异步测试需要）
def pytest_configure(config):
    config.addinivalue_line("markers", "asyncio: mark test as async")


# 沙箱安全覆盖：pytest 内置 tmp_path 在 Windows 上通过 \\?\ 扩展路径前缀
# 创建/清理临时目录，部分受限执行环境不识别该形式的工作区路径而拒绝访问。
# 此覆盖改用仓库内普通路径 .pytest-tmp/<用例>-<随机>，行为等价（每用例独立
# 临时目录，teardown 尽力清理）；在正常开发机上与内置实现无差别。
_SAFE_TMP_ROOT = Path(__file__).resolve().parent.parent / ".pytest-tmp"


@pytest.fixture
def tmp_path(request: pytest.FixtureRequest) -> Path:
    import pytest as _pytest

    node_name = "".join(c if c.isalnum() or c in "-_" else "_" for c in request.node.name)[:80]
    directory = _SAFE_TMP_ROOT / f"{node_name}-{uuid.uuid4().hex[:8]}"
    directory.mkdir(parents=True, exist_ok=True)
    yield directory
    shutil.rmtree(directory, ignore_errors=True)
