"""群内通知：事务型 Outbox 投递 + 立即文本回复（计划书 §12.1）。

- 执行器已在业务事务内预写 notification_deliveries(PENDING)，提交后由
  :meth:`flush` 逐条投递（加载工单 → 生成文案 → 发送 → 标记 SENT/FAILED）。
- 非执行器类回复（澄清/确认提示/校验拒绝）走 :meth:`send_group_now` 同步发送，
  同时落一条审计 Outbox 记录。
- sender 是可注入的可调用 ``(target_id, text) -> None``；生产环境包装 dws。
"""

from __future__ import annotations

from typing import Any, Callable

from db import Database
from logger import get_logger
from tickets.commands import reply_text

logger = get_logger(__name__)

Sender = Callable[[str, str], None]


class Notifier:
    def __init__(self, db: Database, sender: Sender) -> None:
        self._db = db
        self._sender = sender

    def flush(self) -> int:
        """投递所有 PENDING Outbox 通知，返回投递数。"""
        delivered = 0
        for item in self._db.claim_pending_notifications(limit=50):
            try:
                text = self._build_text(item)
                self._sender(item["target_id"], text)
                self._db.mark_notification(item["id"], "SENT")
                delivered += 1
            except Exception as exc:
                logger.warning("通知投递失败 id=%s err=%s", item["id"], exc)
                self._db.mark_notification(item["id"], "FAILED", error=str(exc))
        return delivered

    def send_group_now(self, group_id: str, text: str, *, message_id: str) -> None:
        """同步发送群文本回复，并落一条已投递的审计 Outbox 记录。"""
        notification_id = self._db.insert_notification(
            dedupe_key=f"reply:{message_id}",
            ticket_id=None,
            notification_type="group_text",
            target_type="group",
            target_id=group_id,
        )
        try:
            self._sender(group_id, text)
            if notification_id:
                self._db.mark_notification(notification_id, "SENT")
        except Exception as exc:
            logger.warning("即时回复发送失败 group=%s err=%s", group_id, exc)

    def _build_text(self, item: dict[str, Any]) -> str:
        ticket = None
        if item["ticket_id"] is not None:
            ticket = self._db.get_ticket(item["ticket_id"])
        return reply_text(item["notification_type"], ticket, {})
