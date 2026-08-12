"""数据模型：事件标准化结构与角色/状态常量。

角色与状态统一用小写枚举值存储，展示时再映射中文。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional


# ───────────────────────── 常量枚举 ─────────────────────────
# 角色（与 config.ROLE_PERMISSIONS 对齐）
ROLE_MANAGER = "MANAGER"        # 店长
ROLE_ENGINEER = "ENGINEER"      # 工程师
ROLE_OTHER = "OTHER"            # 其他成员（只归档）
ROLE_SYSTEM = "SYSTEM"          # 系统监听账号
ROLE_UNKNOWN = "UNKNOWN"        # 未在群配置中的发送人

# 工单状态
TICKET_ACTIVE = "ACTIVE"
TICKET_OVERDUE = "ACTIVE_OVERDUE"
TICKET_COMPLETED = "COMPLETED"
TICKET_CANCELLED = "CANCELLED"  # v4.0: #取消工单 或 AI 取消确认后的终态

# 等待责任方
WAITING_ENGINEER_SIDE = "ENGINEER_SIDE"
WAITING_MANAGER_SIDE = "MANAGER_SIDE"
WAITING_NONE = "NONE"

# 消息类型
MSG_TEXT = "text"
MSG_IMAGE = "image"
MSG_FILE = "file"
MSG_RICH = "rich"

# 责任周期状态
CYCLE_PENDING = "PENDING"
CYCLE_CLAIMED = "CLAIMED"
CYCLE_SENT = "SENT"
CYCLE_CANCELLED = "CANCELLED"

# 超时周期状态
TIMEOUT_WAITING_REASON = "WAITING_REASON"
TIMEOUT_EXTENDED = "EXTENDED"

# 通知状态
NOTIFY_PENDING = "PENDING"
NOTIFY_SENT = "SENT"
NOTIFY_FAILED = "FAILED"
NOTIFY_CANCELLED = "CANCELLED"


@dataclass
class NormalizedMessage:
    """标准化后的群消息事件（计划书 14.3）。

    sender_id 统一为 openDingtalkId（Phase 0 实测：事件只提供
    sender_open_dingtalk_id，不提供 userId）。
    reply_to_message_id 从钉钉回复/引用字段提取，用于多工单路由（v4.0）。
    """

    message_id: str
    group_id: str
    sender_id: str
    sender_name: str
    content: str
    message_type: str = MSG_TEXT
    sent_at: Optional[datetime] = None
    sender_role: str = ROLE_UNKNOWN
    is_self: bool = False
    reply_to_message_id: Optional[str] = None  # v4.0: 钉钉回复/引用目标消息 ID
    raw_event: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.sent_at is None:
            self.sent_at = datetime.now()

    def brief(self) -> str:
        """日志用简短描述，不包含原文大段内容。"""
        reply = f" reply_to={self.reply_to_message_id[:8]}..." if self.reply_to_message_id else ""
        return (
            f"msg={self.message_id} group={self.group_id} "
            f"sender={self.sender_name}({self.sender_id[:8]}) "
            f"role={self.sender_role} type={self.message_type} "
            f"sent_at={self.sent_at.isoformat() if self.sent_at else '?'}"
            f"{reply}"
        )
