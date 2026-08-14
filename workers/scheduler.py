"""后台调度器（计划书 §9.5、Task 10）。

周期任务：
1. 待确认动作过期清理（EXPIRED）；
2. SLA 时效提醒：活动工单时效临近到期（前 N 小时）提醒一次，超时后再提醒一次。
   提醒按 dedupe_key 去重，同一工单同一 deadline 只发一次。

用户业务决策（2026-08-12）：不做口语完工关键词；「修好了但没汇报」的情况
由到期提醒兜住 —— 提醒报修人时效将尽，请在群里回复「修好了」完成工单。
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from typing import Any

from config import SLA_REMIND_BEFORE_HOURS, SLA_SCAN_INTERVAL_SECONDS
from db import Database
from logger import get_logger

logger = get_logger(__name__)


class SchedulerWorker:
    def __init__(
        self,
        *,
        db: Database,
        notifier: Any,
        interval: float = SLA_SCAN_INTERVAL_SECONDS,
        remind_before_hours: float = SLA_REMIND_BEFORE_HOURS,
    ) -> None:
        self._db = db
        self._notifier = notifier
        self._interval = interval
        self._remind_before_hours = remind_before_hours

    async def run(self) -> None:
        logger.info(
            "调度器启动 interval=%ss remind_before=%sh",
            self._interval, self._remind_before_hours,
        )
        while True:
            try:
                await asyncio.sleep(self._interval)
                self.scan(datetime.now())
            except asyncio.CancelledError:
                logger.info("调度器停止")
                raise
            except Exception as exc:
                logger.warning("调度器扫描异常 err=%s", exc)

    def scan(self, now: datetime) -> None:
        """执行一轮周期任务。"""
        self.scan_pending_expiry(now)
        self.scan_sla_reminders(now)
        self.scan_order_status()

    def scan_order_status(self) -> int:
        """读订单↔门店共享表，检测状态变化并群内通知一次。

        通知规则（用户业务决策）：
        - 状态变为「卖家已发货」→ 提醒一次「订单已发货」（不再问签收）；
        - 状态变为「关闭」（如未付款关闭）→ 提醒一次「订单因未付款已关闭」。
        """
        from config import ORDER_STORE_TABLE_PATH
        from reconciling.order_store import read_order_rows

        notified = 0
        for row in read_order_rows(ORDER_STORE_TABLE_PATH):
            order_id = str(row.get("order_id") or "").strip()
            status = str(row.get("status") or "").strip()
            if not order_id or not status:
                continue
            monitor = self._db.get_order_monitor(order_id)
            if monitor is None:
                continue  # 非本系统提交的订单，跳过
            if status == monitor["last_status"]:
                continue  # 状态未变，不重复通知

            ticket = self._db.get_ticket(monitor["ticket_id"])
            ticket_no = monitor["ticket_no"]
            if ticket is None:
                logger.warning("订单对应工单不存在 order=%s ticket_id=%s", order_id, monitor["ticket_id"])
                self._db.update_order_status(order_id, status)
                continue
            group_id = ticket["group_id"]

            if "发货" in status and not monitor["shipped_notified"]:
                tracking = row.get("tracking_number")
                text = (
                    f"📦 订单 {order_id} 已发货，工单 {ticket_no} 请留意收货。"
                    + (f"\n物流单号：{tracking}" if tracking else "")
                )
                if group_id:
                    self._notifier.send_group_now(group_id, text, message_id=f"order-shipped:{order_id}")
                notified += 1
                self._db.update_order_status(order_id, status, shipped_notified=True)
                logger.info("订单已发货通知 order=%s ticket=%s", order_id, ticket_no)
            elif "关闭" in status and not monitor["closed_notified"]:
                text = (
                    f"⚠️ 订单 {order_id} 因未付款已关闭，工单 {ticket_no} "
                    f"请及时处理（重新下单或更换维修方式）。"
                )
                if group_id:
                    self._notifier.send_group_now(group_id, text, message_id=f"order-closed:{order_id}")
                notified += 1
                self._db.update_order_status(order_id, status, closed_notified=True)
                logger.info("订单关闭通知 order=%s ticket=%s", order_id, ticket_no)
            else:
                # 其他状态变化（如付款/待发货）只更新 last_status，不通知
                self._db.update_order_status(order_id, status)
                logger.info("订单状态更新（不通知）order=%s old=%r new=%r ticket=%s",
                            order_id, monitor["last_status"], status, ticket_no)
        return notified

    def scan_pending_expiry(self, now: datetime) -> int:
        """待确认动作超时置 EXPIRED。"""
        from routing.pending_actions import PendingActionService

        expired = PendingActionService(self._db).expire_due(now)
        if expired:
            logger.info("待确认动作过期清理 count=%d", expired)
        return expired

    def scan_sla_reminders(self, now: datetime) -> int:
        """活动工单时效临近到期/已超时 → 去重群提醒。"""
        window = (now + timedelta(hours=self._remind_before_hours)).strftime("%Y-%m-%d %H:%M:%S")
        now_str = now.strftime("%Y-%m-%d %H:%M:%S")
        rows = self._db.connect().execute(
            """SELECT * FROM tickets
               WHERE status IN ('ACTIVE', 'ACTIVE_OVERDUE') AND current_deadline_at <= ?""",
            (window,),
        ).fetchall()
        sent = 0
        for row in rows:
            ticket = dict(row)
            overdue = ticket["current_deadline_at"] < now_str
            if overdue:
                dedupe = f"sla_overdue:{ticket['id']}"
                text = (
                    f"⏰ 工单 {ticket['ticket_no']} 已超时效（{ticket['current_deadline_at']}）。"
                    f"如已修复，请回复「修好了」完成工单；如未完成请说明情况。"
                )
            else:
                dedupe = f"sla_remind:{ticket['id']}:{ticket['current_deadline_at']}"
                text = (
                    f"⏰ 工单 {ticket['ticket_no']} 时效即将到期（{ticket['current_deadline_at']}）。"
                    f"如已修复，请回复「修好了」完成工单。"
                )
            if self._notifier.send_deduped_group(ticket["group_id"], text, dedupe_key=dedupe):
                sent += 1
                logger.info(
                    "SLA 提醒已发送 ticket=%s overdue=%s deadline=%s",
                    ticket["ticket_no"], overdue, ticket["current_deadline_at"],
                )
        return sent
