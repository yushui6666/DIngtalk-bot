"""快递签收确认（业务：淘宝采购后需向发起人确认快递是否签收）。

触发：工程师提交维修方式且带订单号 → 创建 WAITING 确认，群内询问发起人。
回复：发起人回复「已签收/收到了」或「未收到/还没到」→ 本地确定性匹配，记录结果。

匹配规则：否定词优先（「没收到」含「收到」），命中即返回，不调用模型。
"""

from __future__ import annotations

from typing import Any

from db import Database
from logger import get_logger

logger = get_logger(__name__)

STATUS_WAITING = "WAITING"
STATUS_SIGNED = "SIGNED"
STATUS_UNSIGNED = "UNSIGNED"

# 否定词优先（避免「没收到」被「收到」误判）
_UNSIGNED_PATTERNS = (
    "没收到", "未收到", "没有收到", "还没到", "没到", "未到",
    "没签收", "未签收", "还没签收", "没有签收", "没到货",
)
_SIGNED_PATTERNS = (
    "已签收", "签收了", "签收", "收到了", "收到货", "拿到", "已经到", "已收到", "到了",
)


def match_reply(content: str) -> str | None:
    """识别签收确认回复：返回 SIGNED / UNSIGNED / None。"""
    text = content.strip()
    if not text:
        return None
    for pattern in _UNSIGNED_PATTERNS:
        if pattern in text:
            return STATUS_UNSIGNED
    for pattern in _SIGNED_PATTERNS:
        if pattern in text:
            return STATUS_SIGNED
    return None


class DeliveryConfirmService:
    def __init__(self, db: Database) -> None:
        self._db = db

    def create(self, ticket_id: int, order_no: str, group_id: str, confirm_user_id: str) -> int:
        """创建待确认记录；同单同单号已 WAITING 时返回 0（不重复询问）。"""
        return self._db.create_delivery_confirmation(
            ticket_id, order_no, group_id, confirm_user_id
        )

    def get_waiting(self, group_id: str, user_id: str) -> dict[str, Any] | None:
        return self._db.get_waiting_delivery_confirmation(group_id, user_id)

    def resolve(self, confirmation_id: int, status: str, message_id: str) -> bool:
        return self._db.resolve_delivery_confirmation(confirmation_id, status, message_id)

    def expire_by_ticket(self, ticket_id: int) -> int:
        return self._db.expire_deliveries_by_ticket(ticket_id)
