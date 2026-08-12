"""pytest 路径配置与插件注册。"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# v4.0: 注册 asyncio 标记（Task 4 model_client 异步测试需要）
def pytest_configure(config):
    config.addinivalue_line("markers", "asyncio: mark test as async")
