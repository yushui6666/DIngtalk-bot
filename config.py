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
# 角色分配（用户确认 2026-08-11）：聂宇清=工程师，暂代工程负责人+区域经理；yushui=店长
# ⚠️ yushui 为外部测试账号，无真实 userId，使用测试占位 userId "yushui-external-test"
#    仅用于测试阶段角色匹配；私聊发送通道对该账号不可达，正式环境替换为组织内店长。
# 角色列表一律使用 userId；事件只提供 openDingtalkId，运行时经 USER_ID_MAP 映射后匹配。
# 群成员（Phase 0 实测）：
#   yushui（群主）  = 外部测试账号，未加入组织，无 userId → 店长（测试占位）
#   聂宇清          openDingtalkId=DV2iipykTJciSappVW4GfsiSQii2iPTIiiIm8jw  userId=1785387642795212
GROUPS = [
    {
        "group_id": "cidO+6f66Jja9EzGTFm3rra1Q==",
        "store_name": "钉钉消息测试",  # 测试群即店名，正式环境替换为实际店名
        "manager_ids": [
            "yushui-external-test",  # yushui（外部账号，测试占位 userId）
        ],
        "engineer_ids": [
            "1785387642795212",  # 聂宇清（工程师，暂代工程负责人+区域经理）
        ],
        "other_member_ids": [],
        "engineering_leader_id": "1785387642795212",  # 聂宇清暂代（用户确认）
        "regional_manager_id": "1785387642795212",  # 聂宇清暂代（用户确认）
        "is_active": True,
    }
]

# openDingtalkId → userId 静态映射（Phase 1 测试用）。
# 正式环境启动流程调用通讯录自动解析并缓存，见 id_mapper.py TODO。
USER_ID_MAP = {
    "DV2iipykTJciSappVW4GfsiSQii2iPTIiiIm8jw": "1785387642795212",  # 聂宇清
    "Dk7Rf4NfFahnD2MHQgAE3gy2iPTIiiIm8jw": "yushui-external-test",  # yushui（外部账号，测试占位）
}
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
SLA_REMIND_BEFORE_HOURS = 6
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
