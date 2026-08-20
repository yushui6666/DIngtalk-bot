"""工单命令执行器（计划书 §9、Task 9）。

在一个 SQLite 短事务内完成：
  唯一 action_executions → 业务变更(CAS) → 消息归属 → 消息落库 →
  processed_events 幂等 → 通知 Outbox 预写 → 执行终态 APPLIED。

乐观版本冲突返回 CommandResult(REJECTED)，不覆盖他人更新。
"""

from __future__ import annotations

from typing import Any, Callable

from db import Database
from logger import get_logger
from models import (
    NormalizedMessage,
    TICKET_ACTIVE,
    TICKET_CANCELLED,
    TICKET_COMPLETED,
    TICKET_STOPPED,
)
from semantics.types import CommandResult, ValidatedCommand
from tickets.commands import reply_text
from tickets.repository import TicketRepository

logger = get_logger(__name__)

LINK_CREATE = "CREATE"
LINK_EXECUTED = "EXECUTED"
RESULT_OK = "OK"
RESULT_REJECTED = "REJECTED"
RESULT_INTERNAL_ERROR = "INTERNAL_ERROR"

# 只读/终态意图：不参与责任方切换（计划书 §9.3：仅有效人工业务消息切换）
_READONLY_INTENTS = frozenset({"ticket.query", "ticket.select", "ticket.complete", "ticket.cancel", "ticket.stop"})


class TicketCommandExecutor:
    def __init__(self, db: Database, repo: TicketRepository) -> None:
        self._db = db
        self._repo = repo

    def execute(
        self,
        command: ValidatedCommand,
        *,
        message: NormalizedMessage | None = None,
        pending_action_id: int | None = None,
        pending_version: int | None = None,
    ) -> CommandResult:
        """执行命令（需在事务外调用，内部自行开启短事务）。"""
        dedupe_key = (
            f"direct:{command.message_id}"
            if pending_action_id is None
            else f"confirm:{pending_action_id}:{pending_version}"
        )
        # 已应用过 → 幂等返回成功（崩溃重放不重复执行）
        if self._db.execution_applied(dedupe_key):
            logger.info("执行记录已应用，跳过 message_id=%s", command.message_id)
            return CommandResult(RESULT_OK, command.target_ticket_id, None, ())

        logger.info(
            "动作执行开始 msg=%s intent=%s target_ticket_id=%s expected_version=%s",
            command.message_id, command.intent, command.target_ticket_id, command.expected_ticket_version,
        )
        try:
            with self._db.transaction(f"execute:{command.intent}"):
                inserted = self._db.insert_execution(
                    dedupe_key=dedupe_key,
                    source_message_id=command.message_id,
                    confirmation_message_id=message.message_id if message else None,
                    pending_action_id=pending_action_id,
                    intent=command.intent,
                    target_ticket_id=command.target_ticket_id,
                    command_json={
                        "intent": command.intent,
                        "fields": command.fields,
                        "target_ticket_id": command.target_ticket_id,
                    },
                )
                # 事务内存在同 key 执行记录（异常重入）→ 直接放弃
                if not inserted:
                    logger.warning("执行记录已存在 dedupe_key=%s", dedupe_key)
                    return CommandResult(RESULT_OK, command.target_ticket_id, None, ())

                result = self._dispatch(command, message)
                if result.status == RESULT_OK:
                    self._db.mark_execution_applied(dedupe_key)
                return result
        except Exception as exc:
            logger.error("执行失败 intent=%s message_id=%s err=%s",
                         command.intent, command.message_id, exc)
            return CommandResult(RESULT_INTERNAL_ERROR, command.target_ticket_id, None, ())

    # ─────────────────────── 分发 ───────────────────────
    def _dispatch(self, command: ValidatedCommand, message: NormalizedMessage | None) -> CommandResult:
        handler = _HANDLERS.get(command.intent)
        if handler is None:
            logger.warning("无执行器 intent=%s", command.intent)
            return CommandResult(RESULT_REJECTED, None, None, ())
        return handler(self, command, message)

    # ─────────────────────── 各意图执行器 ───────────────────────
    def _execute_create(self, command: ValidatedCommand, message: NormalizedMessage | None) -> CommandResult:
        group = self._db.get_group(command.group_id)
        if group is None:
            return CommandResult(RESULT_INTERNAL_ERROR, None, None, ())
        fields = command.fields
        sla_label = fields.get("sla")
        if not sla_label:
            # sla 已设为必填（2026-08-19）；绕过校验走到这里视为拒绝
            return CommandResult(RESULT_REJECTED, None, None, ())
        ticket_id = self._repo.create_ticket(
            group=group,
            reporter_id=command.actor_id,
            subject=fields.get("subject") or "未命名",
            location=fields.get("location") or "",
            problem_description=fields.get("problem_description") or "",
            sla_label=sla_label,
            now=self._now(),
        )
        ticket = self._db.get_ticket(ticket_id)
        self._finalize(command, ticket, LINK_CREATE, message)
        return CommandResult(RESULT_OK, ticket_id, ticket["version"], ())

    def _execute_add_detail(self, command: ValidatedCommand, message: NormalizedMessage | None) -> CommandResult:
        ticket = self._require_ticket(command)
        if ticket is None:
            return CommandResult(RESULT_INTERNAL_ERROR, None, None, ())
        content = (message.content if message else "") or command.fields.get("content", "")
        new_desc = (ticket["problem_description"] or "")
        if content:
            new_desc = f"{new_desc}\n{content}".strip()
        ok = self._db.update_ticket_cas(
            ticket["id"], ticket["version"],
            "problem_description=?, last_business_event_at=?, last_business_message_id=?",
            (new_desc, self._now(), command.message_id),
        )
        if not ok:
            return CommandResult(RESULT_REJECTED, ticket["id"], ticket["version"], ())
        updated = self._db.get_ticket(ticket["id"])
        self._finalize(command, updated, LINK_EXECUTED, message)
        return CommandResult(RESULT_OK, updated["id"], updated["version"], ())

    def _execute_submit_version(
        self,
        command: ValidatedCommand,
        message: NormalizedMessage | None,
        *,
        kind: str,
    ) -> CommandResult:
        ticket = self._require_ticket(command)
        if ticket is None:
            return CommandResult(RESULT_INTERNAL_ERROR, None, None, ())
        fields = command.fields
        suppress_notify = False
        if kind == "diagnosis":
            self._db.add_diagnosis_version(ticket["id"], command.message_id,
                                           list(fields.get("diagnosis_items", [])), command.actor_id)
        elif kind == "repair":
            repair_method = fields.get("repair_method", "")
            if repair_method:
                self._db.add_repair_method_version(
                    ticket["id"], command.message_id,
                    repair_method, fields.get("order_no"), command.actor_id)
            else:
                # 只发订单号（无维修方式，如裸单号/诊断+订单消息）：不写空的维修方式版本，
                # 也不发「已记录维修方式」通知（订单登记+延期由 pipeline 负责并已回执）
                suppress_notify = True
            # 同一消息顺带提交了故障判断（如「估计是铰链坏了，单号是…」）→ 一并记录
            diagnosis_items = fields.get("diagnosis_items")
            if diagnosis_items:
                self._db.add_diagnosis_version(
                    ticket["id"], command.message_id,
                    [str(x) for x in diagnosis_items], command.actor_id)
        elif kind == "timeout":
            # 计划书 §4.8：仅当存在尚未解释的超时周期时接受原因
            if not self._db.add_timeout_cycle_reason(
                ticket["id"], command.message_id,
                fields.get("timeout_reason", ""), command.actor_id,
            ):
                logger.info("无未解释超时周期，拒绝原因提交 ticket=%s msg=%s",
                            ticket["ticket_no"], command.message_id)
                return CommandResult(RESULT_REJECTED, ticket["id"], ticket["version"], ())
        # 业务变更 → 推进版本与顺序游标
        ok = self._bump_version(ticket, command)
        if not ok:
            return CommandResult(RESULT_REJECTED, ticket["id"], ticket["version"], ())
        updated = self._db.get_ticket(ticket["id"])
        self._finalize(command, updated, LINK_EXECUTED, message, notify=not suppress_notify)
        return CommandResult(RESULT_OK, updated["id"], updated["version"], ())

    def _execute_complete(self, command: ValidatedCommand, message: NormalizedMessage | None) -> CommandResult:
        ticket = self._require_ticket(command)
        if ticket is None:
            return CommandResult(RESULT_INTERNAL_ERROR, None, None, ())
        ok = self._db.update_ticket_cas(
            ticket["id"], ticket["version"],
            "status=?, closed_at=?, last_business_event_at=?, last_business_message_id=?",
            (TICKET_COMPLETED, self._now(), self._now(), command.message_id),
        )
        if not ok:
            return CommandResult(RESULT_REJECTED, ticket["id"], ticket["version"], ())
        self._db.close_responsibility_cycles(ticket["id"], command.message_id)
        self._db.clear_contexts_by_ticket(ticket["id"])
        updated = self._db.get_ticket(ticket["id"])
        self._finalize(command, updated, LINK_EXECUTED, message)
        return CommandResult(RESULT_OK, updated["id"], updated["version"], ())

    def _execute_cancel(self, command: ValidatedCommand, message: NormalizedMessage | None) -> CommandResult:
        ticket = self._require_ticket(command)
        if ticket is None:
            return CommandResult(RESULT_INTERNAL_ERROR, None, None, ())
        ok = self._db.update_ticket_cas(
            ticket["id"], ticket["version"],
            "status=?, closed_at=?, cancelled_at=?, cancelled_by=?, cancel_reason=?,"
            " last_business_event_at=?, last_business_message_id=?",
            (TICKET_CANCELLED, self._now(), self._now(), command.actor_id,
             command.fields.get("cancel_reason", ""), self._now(), command.message_id),
        )
        if not ok:
            return CommandResult(RESULT_REJECTED, ticket["id"], ticket["version"], ())
        self._db.close_responsibility_cycles(ticket["id"], command.message_id)
        self._db.clear_contexts_by_ticket(ticket["id"])
        updated = self._db.get_ticket(ticket["id"])
        self._finalize(command, updated, LINK_EXECUTED, message)
        return CommandResult(RESULT_OK, updated["id"], updated["version"], ())

    def _execute_stop(self, command: ValidatedCommand, message: NormalizedMessage | None) -> CommandResult:
        """#停止维修：工程负责人确认不再维修 → STOPPED 终态，强制记录停修原因。"""
        ticket = self._require_ticket(command)
        if ticket is None:
            return CommandResult(RESULT_INTERNAL_ERROR, None, None, ())
        ok = self._db.update_ticket_cas(
            ticket["id"], ticket["version"],
            "status=?, closed_at=?, stopped_at=?, stopped_by=?, stop_reason=?,"
            " last_business_event_at=?, last_business_message_id=?",
            (TICKET_STOPPED, self._now(), self._now(), command.actor_id,
             command.fields.get("stop_reason", ""), self._now(), command.message_id),
        )
        if not ok:
            return CommandResult(RESULT_REJECTED, ticket["id"], ticket["version"], ())
        self._db.close_responsibility_cycles(ticket["id"], command.message_id)
        self._db.clear_contexts_by_ticket(ticket["id"])
        updated = self._db.get_ticket(ticket["id"])
        self._finalize(command, updated, LINK_EXECUTED, message)
        return CommandResult(RESULT_OK, updated["id"], updated["version"], ())

    def _execute_reopen(self, command: ValidatedCommand, message: NormalizedMessage | None) -> CommandResult:
        ticket = self._require_ticket(command)
        if ticket is None:
            return CommandResult(RESULT_INTERNAL_ERROR, None, None, ())
        ok = self._db.update_ticket_cas(
            ticket["id"], ticket["version"],
            "status=?, closed_at=NULL, reopen_count=reopen_count+1,"
            " waiting_side='NONE', waiting_since=NULL, current_responsibility_cycle_id=NULL,"
            " last_business_event_at=?, last_business_message_id=?",
            (TICKET_ACTIVE, self._now(), command.message_id),
        )
        if not ok:
            return CommandResult(RESULT_REJECTED, ticket["id"], ticket["version"], ())
        # 计划书 §9.1：重开后建立新的责任周期；同时清理 SLA 去重键，
        # 让重开后的工单可再次收到到期/超时提醒并按需要建立超时周期
        self._db.close_responsibility_cycles(ticket["id"], command.message_id)
        self._db.clear_ticket_sla_dedupe(ticket["id"])
        updated = self._db.get_ticket(ticket["id"])
        self._finalize(command, updated, LINK_EXECUTED, message)
        return CommandResult(RESULT_OK, updated["id"], updated["version"], ())

    def _execute_query(self, command: ValidatedCommand, message: NormalizedMessage | None) -> CommandResult:
        ticket = self._require_ticket(command)
        if ticket is None:
            return CommandResult(RESULT_OK, None, None, ())
        self._finalize(command, ticket, LINK_EXECUTED, message)
        return CommandResult(RESULT_OK, ticket["id"], ticket["version"], ())

    def _execute_select(self, command: ValidatedCommand, message: NormalizedMessage | None) -> CommandResult:
        ticket = self._require_ticket(command)
        if ticket is None:
            return CommandResult(RESULT_INTERNAL_ERROR, None, None, ())
        # 建立用户上下文（默认 30 分钟）
        from datetime import datetime, timedelta

        now = datetime.now()
        self._db.set_ticket_context(
            command.group_id, command.actor_id, ticket["id"],
            f"{now.strftime('%Y-%m-%d %H:%M:%S')}|{command.message_id}",
            (now + timedelta(minutes=30)).strftime("%Y-%m-%d %H:%M:%S"),
            now=now.strftime("%Y-%m-%d %H:%M:%S"),
        )
        self._finalize(command, ticket, LINK_EXECUTED, message)
        return CommandResult(RESULT_OK, ticket["id"], ticket["version"], ())

    # ─────────────────────── 公共步骤 ───────────────────────
    def _require_ticket(self, command: ValidatedCommand) -> dict[str, Any] | None:
        if command.target_ticket_id is None:
            return None
        return self._db.get_ticket(command.target_ticket_id)

    def _bump_version(self, ticket: dict[str, Any], command: ValidatedCommand) -> bool:
        return self._db.update_ticket_cas(
            ticket["id"], ticket["version"],
            "last_business_event_at=?, last_business_message_id=?",
            (self._now(), command.message_id),
        )

    def _finalize(
        self,
        command: ValidatedCommand,
        ticket: dict[str, Any],
        link_type: str,
        message: NormalizedMessage | None,
        notify: bool = True,
    ) -> None:
        """消息归属 + 消息落库 + 幂等记录 + 通知预写（同事务）。"""
        self._db.link_message(command.message_id, ticket["id"], link_type, 0.0)
        if message is not None:
            self._db.add_ticket_message(
                command.message_id, ticket["id"], command.actor_id, command.actor_role,
                message.content, message.message_type,
                message.sent_at.strftime("%Y-%m-%d %H:%M:%S"),
            )
            # 计划书 §9.3：业务动作（非查询/选择）按消息发送方角色切换责任方
            if command.intent not in _READONLY_INTENTS:
                self._db.switch_responsibility(
                    ticket["id"], command.actor_role, command.message_id,
                    message.sent_at.strftime("%Y-%m-%d %H:%M:%S"),
                )
        self._db.record_processed_event(command.message_id, command.group_id, "EXECUTED")
        if notify:
            self._db.insert_notification(
                dedupe_key=f"exec:{command.message_id}",
                ticket_id=ticket["id"],
                notification_type=command.intent,
                target_type="group",
                target_id=command.group_id,
            )

    def _now(self) -> str:
        from datetime import datetime

        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


_HANDLERS: dict[str, Callable[..., CommandResult]] = {
    "ticket.create": TicketCommandExecutor._execute_create,
    "ticket.add_detail": TicketCommandExecutor._execute_add_detail,
    "ticket.diagnosis.submit": lambda self, c, m: self._execute_submit_version(c, m, kind="diagnosis"),
    "ticket.repair_plan.submit": lambda self, c, m: self._execute_submit_version(c, m, kind="repair"),
    "ticket.timeout_reason.submit": lambda self, c, m: self._execute_submit_version(c, m, kind="timeout"),
    "ticket.complete": TicketCommandExecutor._execute_complete,
    "ticket.cancel": TicketCommandExecutor._execute_cancel,
    "ticket.stop": TicketCommandExecutor._execute_stop,
    "ticket.reopen": TicketCommandExecutor._execute_reopen,
    "ticket.query": TicketCommandExecutor._execute_query,
    "ticket.select": TicketCommandExecutor._execute_select,
}
