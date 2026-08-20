"""后台调度器（计划书 §9.5、Task 10）。

周期任务：
1. 待确认动作过期清理（EXPIRED）；
2. SLA 时效提醒：活动工单时效临近到期（前 N 小时）提醒一次，超时后再提醒一次。
   提醒按 dedupe_key 去重，同一工单同一 deadline 只发一次。
3. 订单到货签收每日提醒：已签收、仍在处理的活动工单每天提醒一次，直至完成。
   签收后不再走 SLA 截止提醒（等货期间才按 SLA 计时，超时由工程师回 #超时原因）。

用户业务决策（2026-08-12/14）：
- 不做口语完工关键词；「修好了但没汇报」的情况由到期提醒兜住。
- 下单不再自动延期；订单签收后开始计时维修，动态计算、每日一催（2026-08-14）。
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from typing import Any

from config import SLA_REMIND_BEFORE_HOURS, SLA_SCAN_INTERVAL_SECONDS
from db import Database
from logger import get_logger

logger = get_logger(__name__)


def _now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


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
        self.scan_received_reminders(now)
        self.scan_order_status()
        self.scan_aitable_sync(now)

    def scan_aitable_sync(self, now: datetime) -> None:
        """把本地工单增量同步到 AI 表格「报修工单」看板数据源（按间隔节流）。

        本地库为真相源：新增/更新自动同步，本地删除的工单自动从线上镜像删除。
        """
        from config import (
            AITABLE_SYNC_ENABLED,
            AITABLE_SYNC_INTERVAL_SECONDS,
            AITABLE_SYNC_PRUNE,
        )

        if not AITABLE_SYNC_ENABLED:
            return
        if getattr(self, "_last_aitable_sync_at", None) is not None:
            elapsed = (now - self._last_aitable_sync_at).total_seconds()
            if elapsed < AITABLE_SYNC_INTERVAL_SECONDS:
                return
        try:
            from workers.aitable_sync import sync_once

            result = sync_once(self._db, prune=AITABLE_SYNC_PRUNE)
            self._last_aitable_sync_at = now
            logger.info(
                "AI 表格同步完成 online=%d create=%d update=%d delete=%d",
                result["online_total"], result["created"],
                result["updated"], result["deleted"],
            )
        except Exception as exc:
            logger.warning("AI 表格同步失败 err=%s", exc)

    def scan_order_status(self) -> int:
        """读订单↔门店共享表，检测状态变化并群内通知一次。

        通知规则（用户业务决策）：
        - 状态变为「卖家已发货」→ 提醒一次「订单已发货」（不再问签收）；
        - 状态变为「关闭」（如未付款关闭）→ 提醒一次「订单因未付款已关闭」；
        - 状态含「签收」或「交易成功」→ 标记已签收、提醒一次「开始计时维修」；
          此后工单每日提醒一次（scan_received_reminders），不再按 SLA 截止提醒。
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
            elif ("签收" in status or "交易成功" in status) and not monitor["received_notified"]:
                # 到货签收 → 一次性提醒「开始计时」，此后每日提醒直至完成
                text = (
                    f"📦 订单 {order_id} 已签收到货，工单 {ticket_no} 开始计时维修，"
                    f"请尽快处理；如已修好请回复「修好了」。"
                )
                if group_id:
                    self._notifier.send_group_now(group_id, text, message_id=f"order-received:{order_id}")
                notified += 1
                self._db.update_order_status(
                    order_id, status,
                    received_at=_now_str(), received_notified=True,
                )
                logger.info("订单签收通知 order=%s ticket=%s", order_id, ticket_no)
            else:
                # 其他状态变化（如付款/待发货/交易成功）只更新 last_status，不通知
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
        """活动工单时效临近到期/已超时 → 去重群提醒。

        订单已签收的工单不在此提醒（改走 scan_received_reminders 每日提醒）。
        """
        window = (now + timedelta(hours=self._remind_before_hours)).strftime("%Y-%m-%d %H:%M:%S")
        now_str = now.strftime("%Y-%m-%d %H:%M:%S")
        rows = self._db.connect().execute(
            """SELECT * FROM tickets t
               WHERE t.status IN ('ACTIVE', 'ACTIVE_OVERDUE') AND t.current_deadline_at <= ?
                 AND t.sla_days > 0   -- 待商榷(0) 不设时效，不参与 SLA 提醒
                 AND NOT EXISTS (
                     SELECT 1 FROM order_monitor om
                     WHERE om.ticket_id = t.id AND om.received_at IS NOT NULL
                 )""",
            (window,),
        ).fetchall()
        sent = 0
        for row in rows:
            ticket = dict(row)
            overdue = ticket["current_deadline_at"] < now_str
            if overdue:
                # 计划书 §9.1：超时工单状态推进 ACTIVE → ACTIVE_OVERDUE（幂等）。
                # 首次推进（返回 True）即建 WAITING_REASON 超时周期，与提醒是否外发成功解耦
                # （影子模式/发送失败时状态已推进，周期必须同步建立，否则工程师回 #超时原因 会被拒）。
                if self._db.mark_ticket_overdue(ticket["id"]):
                    self._db.open_timeout_cycle(ticket["id"], now_str)
                dedupe = f"sla_overdue:{ticket['id']}"
                text = (
                    f"⏰ 工单 {ticket['ticket_no']} 已超时效（{ticket['current_deadline_at']}）。"
                    f"如已修复，请回复「修好了」完成工单；如未完成，请回复 #超时原因 说明情况。"
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

    def scan_received_reminders(self, now: datetime) -> int:
        """订单已签收、仍在处理的活动工单 → 每天提醒一次，直到完成。

        按 (ticket_id, 日期) 去重，一天只提醒一次；不设截止限制
        （用户决策 2026-08-14：签收后开始计时，动态计算、每日一催）。
        """
        date_key = now.strftime("%Y-%m-%d")
        sent = 0
        for ticket in self._db.list_received_active_tickets():
            dedupe = f"received_daily:{ticket['id']}:{date_key}"
            text = (
                f"📦 工单 {ticket['ticket_no']} 的配件已签收，仍在处理中。"
                f"如已修好请回复「修好了」；需要更多时间请回复 #超时原因 说明。"
            )
            if self._notifier.send_deduped_group(ticket["group_id"], text, dedupe_key=dedupe):
                sent += 1
                logger.info("签收后每日提醒 ticket=%s date=%s", ticket["ticket_no"], date_key)
        return sent
