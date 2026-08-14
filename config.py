"""全局配置。

以计划书第 17 节为基础，填入 Phase 0 实测数据（2026-08-11）。

⚠️ 角色分配为占位配置，待用户确认后更新：
- 店长 / 工程师 / 工程负责人 / 区域经理 均为 openDingtalkId。
- 监听账号 = 「工程部AI」= LISTENER_USER_ID。
"""

from __future__ import annotations

import json
import os as _os
from pathlib import Path

from logger import get_logger

logger = get_logger(__name__)

# ───────────────────────── 路径 ─────────────────────────
BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "data" / "tickets.db"
ARCHIVE_DIR = BASE_DIR / "archives"
STORE_TABLE_DIR = BASE_DIR / "data" / "stores"
SUMMARY_OUTPUT_PATH = BASE_DIR / "data" / "summary" / "维修工单汇总.json"
LOG_DIR = BASE_DIR / "logs"
LOG_LEVEL = "INFO"

# 群与成员配置文件（50 群/几百人规模用，可被环境变量 GROUPS_CONFIG_PATH 覆盖）
# - 生产：data/groups.json（全部门店群）
# - 测试：data/group-test.json（测试群，通过 --test 或 GROUPS_CONFIG_PATH 切换）
DEFAULT_GROUPS_CONFIG_PATH = Path(
    _os.environ.get(
        "GROUPS_CONFIG_PATH",
        str(BASE_DIR / "data" / "groups.json"),
    )
)
GROUPS_TEST_CONFIG_PATH = Path(
    _os.environ.get(
        "GROUPS_TEST_CONFIG_PATH",
        str(BASE_DIR / "data" / "group-test.json"),
    )
)

# 淘宝对账导入源文件（「淘宝订单自动下载与地址对账」工具产出，可被环境变量覆盖）
TAOBAO_ORDER_DETAIL_XLSX = Path(
    _os.environ.get(
        "TAOBAO_ORDER_DETAIL_XLSX",
        "/Users/yushui/Desktop/淘宝对账/订单地址明细.xlsx",
    )
)
TAOBAO_PENDING_XLSX = Path(
    _os.environ.get(
        "TAOBAO_PENDING_XLSX",
        "/Users/yushui/Desktop/淘宝对账/待人工处理.xlsx",
    )
)

# 订单↔门店共享表：报修工单提交订单号时写入，另一个 AI 每天回传订单状态
# （可被环境变量 ORDER_STORE_TABLE_PATH 覆盖）
ORDER_STORE_TABLE_PATH = Path(
    _os.environ.get(
        "ORDER_STORE_TABLE_PATH",
        "/Users/yushui/Desktop/淘宝对账/订单门店状态表.xlsx",
    )
)

# 识别到订单号后工单自动延期天数（每单一次）
ORDER_EXTEND_DAYS = 3

# ───────────────────────── 系统监听账号 ─────────────────────────
# 工程部AI 的 openDingtalkId（Phase 0 实测）
LISTENER_USER_ID = "DuT5LjNZRjS6gMdv9dii9LLC2iPTIiiIm8jw"
LISTENER_USER_NAME = "工程部AI"

# ───────────────────────── 群配置 ─────────────────────────
# 群与成员配置独立保存在 JSON 文件（结构：{"groups":[...], "user_id_map":{...}}），
# 便于 50 群/几百人规模维护；GROUPS 与 USER_ID_MAP 由此加载，保持导出名不变。
# - 默认（生产）读取 data/groups.json
# - 测试模式（main.py --test）读取 data/group-test.json
# 角色分配（用户确认 2026-08-11）：聂宇清=工程师，暂代工程负责人+区域经理；yushui=店长
#   yushui 为外部测试账号，无真实 userId，使用测试占位 userId "yushui-external-test"
# 测试群（用户确认 2026-08-13）：朱兴福=店长，聂宇清=工程师
# 角色列表一律使用 userId；事件只提供 openDingtalkId，运行时经 USER_ID_MAP 映射后匹配。

# 当前生效的群配置文件（默认为生产配置；调用 set_groups_config() 可切换）
GROUPS_CONFIG_PATH = DEFAULT_GROUPS_CONFIG_PATH

# 是否处于测试模式（读取 group-test.json）
_is_test_mode = False


def _load_groups_config(path: Path | None = None) -> tuple[list[dict], dict[str, str]]:
    """从群配置文件加载群配置与 userId 映射。"""
    source = path or GROUPS_CONFIG_PATH
    if not source.exists():
        raise SystemExit(f"[config] 群配置文件缺失: {source}")
    raw = json.loads(source.read_text(encoding="utf-8"))
    groups = raw.get("groups") or []
    user_id_map = raw.get("user_id_map") or {}
    if not isinstance(groups, list) or not isinstance(user_id_map, dict):
        raise SystemExit(f"[config] 群配置格式错误: {source}")
    logger.info("群配置已从文件加载 path=%s groups=%d users=%d",
                source, len(groups), len(user_id_map))
    return groups, user_id_map


def set_groups_config(*, test: bool = False, path: str | None = None) -> None:
    """切换群配置来源。

    Args:
        test: True 使用测试配置文件 data/group-test.json。
        path: 显式指定群配置文件路径（优先于 test）；
              传空字符串/None 且非 test 时恢复默认生产配置。
    """
    global GROUPS_CONFIG_PATH, USER_ID_MAP, GROUPS, _is_test_mode
    if path:
        GROUPS_CONFIG_PATH = Path(path)
    elif test:
        GROUPS_CONFIG_PATH = GROUPS_TEST_CONFIG_PATH
    else:
        GROUPS_CONFIG_PATH = DEFAULT_GROUPS_CONFIG_PATH
    _is_test_mode = test
    GROUPS, USER_ID_MAP = _load_groups_config()
    _validate_groups()
    logger.info("群配置已切换 test=%s path=%s", _is_test_mode, GROUPS_CONFIG_PATH)


GROUPS, USER_ID_MAP = _load_groups_config()

# 备用：历史测试群成员（cidBkhYa...）通讯录已解析，正式角色配置时可参考：
#   王建耀(中级工程师)  oid=DK5gfPiPQt9JhftbQjPTSmpGBeH1hOf3Al  userId=220039292529211921
#   徐勇杰(中级工程师)  oid=DBurLHRPMVyAnblOOaHiPHO2BeH1hOf3Al  userId=16245203427839890
#   任柏松(工程总监)    oid=DphiSuoLdZOMZftbQjPTSmpGBeH1hOf3Al  userId=221659554520280778
#   梁佳霓(设计主管)    oid=DOSOxyjJKlfonblOOaHiPHO2BeH1hOf3Al  userId=16220780171019307

# 启动时校验：同一 userId 不得同时出现在店长和工程师列表
def _validate_groups() -> None:
    for g in GROUPS:
        overlap = set(g["manager_ids"]) & set(g["engineer_ids"])
        if overlap:
            raise SystemExit(
                f"[config] 角色重叠 group={g['group_id']} overlap={overlap}，"
                f"同一 openDingtalkId 不能同时是店长和工程师"
            )
        if not g["store_name"]:
            raise SystemExit(f"[config] 群 {g['group_id']} 缺少 store_name")


# ───────────────────────── 关键词 ─────────────────────────
# ⚠️ v3.0 硬编码常量——v4.0 起关键词和角色权限由语义协议 JSON 统一管理，
# 不再从 config 读取。以下常量在 Task 2 (keyword_matcher) 落成后废弃，
# 新代码统一走 protocol_loader 和 TicketProtocol。
REPORT_KEYWORD = "#报修"
DIAGNOSIS_KEYWORD = "#故障判断"
REPAIR_METHOD_KEYWORD = "#维修方式"
TIMEOUT_REASON_KEYWORD = "#超时原因"
COMPLETE_KEYWORD = "#完毕"

KEYWORDS = [REPORT_KEYWORD, DIAGNOSIS_KEYWORD, REPAIR_METHOD_KEYWORD,
            TIMEOUT_REASON_KEYWORD, COMPLETE_KEYWORD]

# 角色 → 允许关键词（v3.0 硬编码；v4.0 改从协议 allowed_roles/actions 派生）
ROLE_PERMISSIONS = {
    "MANAGER": [REPORT_KEYWORD, COMPLETE_KEYWORD],
    "ENGINEER": [DIAGNOSIS_KEYWORD, REPAIR_METHOD_KEYWORD, TIMEOUT_REASON_KEYWORD, COMPLETE_KEYWORD],
    "OTHER": [],
    "SYSTEM": [],
}

# ───────────────────────── 计时参数 ─────────────────────────
SIDE_REPLY_TIMEOUT_HOURS = 4
WEEKEND_ESCALATION_DEFER_HOUR = 9
BACKGROUND_SCAN_INTERVAL_SECONDS = 60

# SLA 提醒：时效临近到期前 N 小时提醒一次；超时后再提醒一次
SLA_REMIND_BEFORE_HOURS = 1
SLA_SCAN_INTERVAL_SECONDS = 60

SLA_OPTIONS = {"1天": 1, "3天": 3, "7天": 7}

REPAIR_METHODS = [
    "淘宝采购后自行维修",
    "需要供应商维修",
    "需要木工维修",
    "需要工程师上门",
    "远程视频维修",
]

ORDER_NO_PLACEHOLDERS = {"无", "暂无", "稍后补", "不知道"}

# ───────────────────────── 云端模型（Task 4 §10.1） ─────────────────────────
# 通过环境变量注入，不在代码或日志中写入 API Key。
# 支持 OpenAI-compatible Chat Completions 接口。
import os as _os

LLM_BASE_URL = _os.environ.get("LLM_BASE_URL", "https://api.openai.com/v1")
LLM_MODEL = _os.environ.get("LLM_MODEL", "gpt-4o-mini")
LLM_TIMEOUT_SECONDS = float(_os.environ.get("LLM_TIMEOUT_SECONDS", "60"))
LLM_RESPONSE_FORMAT = _os.environ.get("LLM_RESPONSE_FORMAT", "auto").lower()
if LLM_RESPONSE_FORMAT not in {"auto", "json_schema", "json_object"}:
    raise ValueError(
        "LLM_RESPONSE_FORMAT 必须是 auto、json_schema 或 json_object"
    )
LLM_MAX_ATTEMPTS = int(_os.environ.get("LLM_MAX_ATTEMPTS", "3"))
LLM_RETRY_DELAYS_SECONDS = [2, 10]  # 计划书 §10.3

# API Key 只从环境变量读取，绝不写入日志或协议 JSON
LLM_API_KEY = _os.environ.get("LLM_API_KEY", "")

# 是否启用云端模型语义匹配（关闭时自然语言走降级路径）
LLM_ENABLED = _os.environ.get("LLM_ENABLED", "true").lower() in ("true", "1", "yes")

# ───────────────────────── 图片附件归档（计划书 §10.6 Task 4A · 存储层） ─────────────────────────
# 消息到达时只归档图片、不调用视觉模型；工单结束后统一分析（用户决策 2026-08-14）。
IMAGE_ARCHIVE_ENABLED = _os.environ.get("IMAGE_ARCHIVE_ENABLED", "true").lower() in ("true", "1", "yes")
IMAGE_ARCHIVE_DIR = Path(_os.environ.get("IMAGE_ARCHIVE_DIR", str(ARCHIVE_DIR / "attachments")))
IMAGE_MAX_BYTES = int(_os.environ.get("IMAGE_MAX_BYTES", str(10 * 1024 * 1024)))
IMAGE_MAX_COUNT_PER_MESSAGE = int(_os.environ.get("IMAGE_MAX_COUNT_PER_MESSAGE", "3"))
IMAGE_DOWNLOAD_TIMEOUT_SECONDS = float(_os.environ.get("IMAGE_DOWNLOAD_TIMEOUT_SECONDS", "15"))
IMAGE_ALLOWED_MIME_TYPES = tuple(
    m.strip()
    for m in _os.environ.get(
        "IMAGE_ALLOWED_MIME_TYPES", "image/jpeg,image/png,image/webp,image/gif,image/bmp"
    ).split(",")
    if m.strip()
)
# 测试/开发可允许本地文件路径作为图片来源；生产保持拒绝
IMAGE_ALLOW_LOCAL_SOURCES = _os.environ.get("IMAGE_ALLOW_LOCAL_SOURCES", "false").lower() in ("true", "1", "yes")


def load_groups() -> list[dict]:
    """加载群配置（当前为常量，后续可改为文件/数据库来源）。"""
    _validate_groups()
    logger.info(
        "群配置加载完成 count=%d store_names=%s listener=%s",
        len(GROUPS),
        [g["store_name"] for g in GROUPS],
        LISTENER_USER_NAME,
    )
    return GROUPS


if __name__ == "__main__":
    # 快速自检：python config.py
    from logger import setup_logging

    setup_logging(level="INFO", log_dir=LOG_DIR)
    load_groups()
    print(json.dumps(GROUPS, ensure_ascii=False, indent=2))
