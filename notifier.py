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
    def __init__(self, db: Database, sender: Sender, *, enabled: bool = True) -> None:
        self._db = db
        self._sender = sender
        self._enabled = enabled
        # 影子模式下仅记录一次每个 dedupe_key，避免每分钟调度重复刷屏
        self._shadow_seen: set[str] = set()

    def flush(self) -> int:
        """投递所有 PENDING Outbox 通知，返回投递数。"""
        delivered = 0
        for item in self._db.claim_pending_notifications(limit=50):
            try:
                text = self._build_text(item)
                if self._enabled:
                    self._sender(item["target_id"], text)
                    self._db.mark_notification(item["id"], "SENT")
                else:
                    # 影子模式：不投递，仅记审计状态，避免群内真实外发
                    logger.info("影子模式：跳过通知投递 id=%s target=%s type=%s",
                                item["id"], item["target_id"], item["notification_type"])
                    self._db.mark_notification(item["id"], "SENT")
                self._db.record_system_reply(
                    item["target_id"], f"sys:notif:{item['id']}", text
                )
                delivered += 1
            except Exception as exc:
                logger.warning("通知投递失败 id=%s err=%s", item["id"], exc)
                self._db.mark_notification(item["id"], "FAILED", error=str(exc))
        return delivered

    def send_group_now(self, group_id: str, text: str, *, message_id: str) -> None:
        """同步发送群文本回复，并落一条已投递的审计 Outbox 记录。

        同时把回执文本作为 SYSTEM 消息写入收件箱，作为群聊上文供模型理解。
        """
        notification_id = self._db.insert_notification(
            dedupe_key=f"reply:{message_id}",
            ticket_id=None,
            notification_type="group_text",
            target_type="group",
            target_id=group_id,
        )
        self._db.record_system_reply(group_id, f"sys:{message_id}", text)
        if not self._enabled:
            logger.info("影子模式：跳过即时回复 group=%s msg=%s text=%r",
                        group_id, message_id, text[:60])
            return
        try:
            self._sender(group_id, text)
            if notification_id:
                self._db.mark_notification(notification_id, "SENT")
        except Exception as exc:
            logger.warning("即时回复发送失败 group=%s err=%s", group_id, exc)

    def send_deduped_group(self, group_id: str, text: str, *, dedupe_key: str) -> bool:
        """按 dedupe_key 去重的群消息（同一 key 只发一次，用于定时提醒）。"""
        if not self._enabled:
            if dedupe_key not in self._shadow_seen:
                self._shadow_seen.add(dedupe_key)
                logger.info("影子模式：跳过去重群消息 group=%s key=%s text=%r",
                            group_id, dedupe_key, text[:60])
            return False
        notification_id = self._db.insert_notification(
            dedupe_key=dedupe_key,
            ticket_id=None,
            notification_type="sla_remind",
            target_type="group",
            target_id=group_id,
        )
        if not notification_id:
            return False
        try:
            self._sender(group_id, text)
            self._db.mark_notification(notification_id, "SENT")
            return True
        except Exception as exc:
            logger.warning("去重群消息发送失败 group=%s err=%s", group_id, exc)
            return False

    def _build_text(self, item: dict[str, Any]) -> str:
        ticket = None
        if item["ticket_id"] is not None:
            ticket = self._db.get_ticket(item["ticket_id"])
        return reply_text(item["notification_type"], ticket, {})
