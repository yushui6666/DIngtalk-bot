"""消息处理管道（计划书 §7、Task 11）。

编排：Inbox → 待确认回复识别 → 关键词快路径/云端模型 → 语义决策审计 →
候选快照 → 路由（编号>引用>上下文>语义>单候选）→ 校验 → 待确认/执行 → Outbox。

运行模式门禁（§16）：
- SHADOW：只记录语义决策，不归属、不建单、不发确认。
- ASSISTED：模型来源动作一律待确认。
- PRODUCTION：按协议确认策略执行。
"""

from __future__ import annotations

import asyncio
import re
from dataclasses import replace
from datetime import datetime
from enum import StrEnum
from typing import Any

from config import LLM_ENABLED, LLM_MAX_ATTEMPTS, LLM_RETRY_DELAYS_SECONDS
from db import Database
from logger import get_logger
from models import NormalizedMessage
from ordering import parse_naive_dt
from routing.pending_actions import PendingActionService
from routing.ticket_contexts import TicketContextStore
from routing.ticket_router import TicketRouter, RoutingConfig
from semantics.keyword_matcher import match_keyword
from semantics.protocol_loader import TicketProtocol
from semantics.types import (
    DecisionStatus,
    PendingActionDraft,
    PendingActionStatus,
    RouteDecision,
    SemanticDecision,
)
from semantics.validator import validate_decision
from tickets.executor import (
    RESULT_OK,
    TicketCommandExecutor,
)
from tickets.repository import TicketRepository

logger = get_logger(__name__)

_INBOX_COMPLETED = "COMPLETED"
_INBOX_RETRY = "RETRY_PENDING"
_INBOX_DEAD = "DEAD_LETTER"


class RuntimeMode(StrEnum):
    SHADOW = "SHADOW"
    ASSISTED = "ASSISTED"
    PRODUCTION = "PRODUCTION"


def _is_model_fallback(decision: SemanticDecision) -> bool:
    return any(e.startswith("model_fallback:") for e in decision.evidence)


class MessageProcessingPipeline:
    def __init__(
        self,
        *,
        db: Database,
        repo: TicketRepository,
        protocol: TicketProtocol,
        router: TicketRouter,
        context: TicketContextStore,
        pending: PendingActionService,
        executor: TicketCommandExecutor,
        notifier: Any,
        classifier: Any | None = None,
        mode: RuntimeMode = RuntimeMode.PRODUCTION,
        llm_enabled: bool = LLM_ENABLED,
        max_attempts: int = LLM_MAX_ATTEMPTS,
        retry_delays: tuple[float, ...] = tuple(LLM_RETRY_DELAYS_SECONDS),
    ) -> None:
        self._db = db
        self._repo = repo
        self._protocol = protocol
        self._router = router
        self._context = context
        self._pending = pending
        self._executor = executor
        self._notifier = notifier
        self._classifier = classifier
        self._mode = mode
        self._llm_enabled = llm_enabled and classifier is not None
        self._max_attempts = max_attempts
        self._retry_delays = retry_delays

    async def process(self, item: dict[str, Any]) -> str:
        """处理一条收件箱消息，返回最终收件箱状态。"""
        msg = _row_to_message(item)
        self._db.inbox_set_status(msg.message_id, "PROCESSING")
        logger.info(
            "消息处理开始 msg=%s group=%s sender=%s role=%s type=%s",
            msg.message_id, msg.group_id, msg.sender_id[:8], msg.sender_role, msg.message_type,
        )
        try:
            status = await self._handle(msg, item)
            logger.info("消息处理完成 msg=%s status=%s", msg.message_id, status)
            return status
        except Exception as exc:
            logger.exception("消息处理异常 message_id=%s err=%s", msg.message_id, exc)
            return self._retry_or_dead(item, msg, str(exc))

    # ─────────────────────── 主流程 ───────────────────────
    async def _handle(self, msg: NormalizedMessage, item: dict[str, Any]) -> str:
        pending = self._pending.get_waiting(msg.group_id, msg.sender_id)

        decision = await self._decide(msg)
        logger.info(
            "语义决策 msg=%s source=%s intent=%s conf=%.2f target_no=%s missing=%s",
            msg.message_id, decision.source, decision.intent, decision.intent_confidence,
            decision.target_ticket_no, decision.missing_fields or "-",
        )
        if pending is not None:
            resolved = await self._resolve_pending_reply(item, msg, pending, decision)
            if resolved is not None:
                return resolved

        # 模型降级 → 重试/死信（关键词快路径不受影响）
        if _is_model_fallback(decision):
            return self._retry_or_dead(item, msg, "模型调用失败")

        # 语义决策审计
        self._save_decision(msg, decision)

        if decision.intent == "chat.ignore":
            return self._complete(item, msg, "IGNORED")

        candidates = self._repo.snapshot_candidates(msg.group_id)

        # 选择工单：建立用户上下文（不走执行器）
        if decision.intent == "ticket.select":
            return self._handle_select(item, msg, decision, candidates)

        # 查询工单：列活动工单（不走执行器）
        if decision.intent == "ticket.query":
            return self._handle_query(item, msg, decision, candidates)

        # 路由
        route = self._router.route(
            message=msg,
            decision=decision,
            candidates=candidates,
            quoted_ticket_id=self._quoted_ticket_id(msg),
            selected_ticket_id=self._context.get_active(msg.group_id, msg.sender_id, datetime.now()),
        )
        logger.info(
            "消息路由 msg=%s route=%s link=%s target_ticket_id=%s candidates=%d",
            msg.message_id, route.decision.value, route.link_type,
            route.target_ticket_id, len(route.candidate_ticket_ids),
        )

        if route.decision == RouteDecision.CLARIFY:
            return self._create_clarify_pending(item, msg, decision, candidates, route)

        # 路由已确定目标（引用/上下文/语义/单候选）但消息未写编号 → 回填到 decision，
        # 使 validator 的必填 ticket_no 与状态校验能通过。
        if (
            route.decision == RouteDecision.ROUTED
            and decision.target_ticket_no is None
            and route.target_ticket_id is not None
        ):
            target_candidate = next(
                (c for c in candidates if c.ticket_id == route.target_ticket_id), None
            )
            if target_candidate is not None:
                decision = replace(decision, target_ticket_no=target_candidate.ticket_no)

        # 校验
        validate_candidates = [c for c in candidates if c.ticket_id == route.target_ticket_id]
        status, cmd, errors = validate_decision(
            decision, message=msg, candidates=validate_candidates, protocol=self._protocol
        )
        logger.info(
            "消息校验 msg=%s status=%s errors=%s",
            msg.message_id, status.value, "；".join(errors) if errors else "-",
        )
        if status == DecisionStatus.VALIDATION_REJECTED:
            return self._reject(item, msg, errors)
        if status == DecisionStatus.IGNORE:
            return self._complete(item, msg, "IGNORED")

        # 协议确认策略（如模型来源 complete/cancel/reopen ALWAYS）→ 待确认
        if status == DecisionStatus.WAITING_CONFIRMATION:
            return self._create_confirm_pending(item, msg, decision, cmd, route, validate_candidates)

        # 运行模式门禁
        if self._mode == RuntimeMode.SHADOW:
            logger.info("影子模式：只记录不执行 message_id=%s intent=%s", msg.message_id, decision.intent)
            return self._complete(item, msg, "SHADOW")

        if self._mode == RuntimeMode.ASSISTED and decision.source == "SEMANTIC_MODEL":
            return self._create_confirm_pending(item, msg, decision, cmd, route, validate_candidates)

        # 执行
        result = self._executor.execute(cmd, message=msg)
        logger.info(
            "动作执行 msg=%s intent=%s result=%s ticket_id=%s version=%s",
            msg.message_id, cmd.intent, result.status, result.ticket_id, result.ticket_version,
        )
        self._complete(item, msg, "EXECUTED" if result.status == RESULT_OK else result.status)
        if result.status == RESULT_OK:
            # 维修方式带订单号 → 延期 + 登记共享表（新流程，替代旧的签收询问）
            self._handle_order_submitted(cmd, result, msg)
            self._notifier.flush()
        else:
            self._notifier.send_group_now(
                msg.group_id, f"工单操作未完成（{result.status}），请重试或联系管理员。",
                message_id=msg.message_id,
            )
        return _INBOX_COMPLETED

    # ─────────────────────── 订单提交处理 ───────────────────────
    def _handle_order_submitted(self, cmd: Any, result: Any, msg: NormalizedMessage) -> None:
        """识别到订单号：该工单自动延期 +3 天（每单一次），并登记到共享表。

        后续由调度器读共享表中另一个 AI 回传的订单状态，发货/关闭时群内通知。
        """
        from config import ORDER_EXTEND_DAYS, ORDER_STORE_TABLE_PATH
        from reconciling.order_store import append_order_row

        if cmd.intent != "ticket.repair_plan.submit" or result.status != RESULT_OK:
            return
        order_no = cmd.fields.get("order_no")
        if not order_no or result.ticket_id is None:
            return
        ticket = self._db.get_ticket(result.ticket_id)
        if ticket is None:
            return

        # 已登记过 → 每单只延期一次，不重复
        if self._db.get_order_monitor(order_no) is not None:
            logger.info("订单已登记过，跳过延期 order=%s ticket=%s", order_no, ticket["ticket_no"])
            return

        extended = self._db.extend_ticket_deadline(
            result.ticket_id, ticket["version"], ORDER_EXTEND_DAYS
        )
        if not extended:
            logger.warning("工单延期失败（版本冲突）order=%s ticket=%s", order_no, ticket["ticket_no"])
        self._db.upsert_order_monitor(
            order_id=order_no, ticket_id=result.ticket_id,
            store=ticket["store_name"], ticket_no=ticket["ticket_no"],
        )
        append_order_row(
            ORDER_STORE_TABLE_PATH, order_id=order_no,
            store=ticket["store_name"], ticket_no=ticket["ticket_no"],
        )

        fresh = self._db.get_ticket(result.ticket_id) if extended else ticket
        text = (
            f"📦 订单 {order_no} 已登记，工单 {ticket['ticket_no']} "
            f"自动延期 {ORDER_EXTEND_DAYS} 天（新截止 {fresh['current_deadline_at']}）。"
        )
        # 若对账表已导入该订单，附注收货地址（增强）
        order = self._db.get_taobao_order(order_no) if hasattr(self._db, "get_taobao_order") else None
        if order and order.get("address"):
            text += f"\n📍 收货地址：{order['address']}"
        self._notifier.send_group_now(msg.group_id, text, message_id=msg.message_id)
        logger.info(
            "订单已登记+延期 order=%s ticket=%s days=%d", order_no, ticket["ticket_no"], ORDER_EXTEND_DAYS
        )

    # ─────────────────────── 决策 ───────────────────────
    async def _decide(self, msg: NormalizedMessage) -> SemanticDecision:
        keyword = match_keyword(msg.content, self._protocol)
        if keyword is not None:
            logger.info("关键词快路径命中 msg=%s intent=%s", msg.message_id, keyword.intent)
            return keyword
        # 订单号快路径：消息基本就是一个淘宝订单号（如「单号 5125…」）→ 视为提交订单
        order_no = _extract_bare_order_no(msg.content)
        if order_no:
            logger.info("订单号快路径命中 msg=%s order_no=%s", msg.message_id, order_no)
            return SemanticDecision(
                protocol_version=self._protocol.protocol_version,
                source="local",
                intent="ticket.repair_plan.submit",
                target_ticket_no=None,
                intent_confidence=1.0,
                fields={"order_no": order_no},
                evidence=("bare_order_no",),
            )
        if not self._llm_enabled:
            logger.info("模型未启用 msg=%s 走降级 ignore", msg.message_id)
            return SemanticDecision(
                protocol_version=self._protocol.protocol_version,
                source="local",
                intent="chat.ignore",
                target_ticket_no=None,
                intent_confidence=0.0,
                evidence=("llm_disabled",),
            )
        candidates = self._repo.snapshot_candidates(msg.group_id)
        pending = self._pending.get_waiting(msg.group_id, msg.sender_id)
        logger.info("调用云端模型 msg=%s candidates=%d", msg.message_id, len(candidates))
        return await self._classifier.classify(msg, candidates=candidates, pending_action=pending)

    def _save_decision(self, msg: NormalizedMessage, decision: SemanticDecision) -> None:
        try:
            self._db.save_semantic_decision(
                msg.message_id,
                protocol_version=decision.protocol_version,
                source=decision.source,
                intent=decision.intent,
                target_ticket_no=decision.target_ticket_no,
                confidence=decision.intent_confidence,
                fields=decision.fields,
                missing_fields=decision.missing_fields,
                evidence=decision.evidence,
            )
        except Exception as exc:
            logger.warning("语义决策审计失败 message_id=%s err=%s", msg.message_id, exc)

    # ─────────────────────── 待确认回复 ───────────────────────
    async def _resolve_pending_reply(
        self, item: dict[str, Any], msg: NormalizedMessage, pending: Any, decision: SemanticDecision
    ) -> str | None:
        if decision.intent == "system.confirm_pending_action":
            logger.info("待确认动作确认 msg=%s pending=%s intent=%s", msg.message_id, pending.id, pending.intent)
            return self._confirm_pending(item, msg, pending)
        if decision.intent == "system.reject_pending_action":
            logger.info("待确认动作拒绝 msg=%s pending=%s intent=%s", msg.message_id, pending.id, pending.intent)
            return self._reject_pending(item, msg, pending)
        if decision.intent == "system.correct_pending_action":
            logger.info("待确认动作修正 msg=%s pending=%s intent=%s", msg.message_id, pending.id, pending.intent)
            return self._correct_pending(item, msg, pending, decision)
        return None

    def _confirm_pending(self, item: dict[str, Any], msg: NormalizedMessage, pending: Any) -> str:
        """确认待执行动作：校验工单版本未变后执行。"""
        # 建单确认：无既有目标，直接按 pending 字段新建
        if pending.intent == "ticket.create":
            cmd = _command_from_pending(msg, pending, None)
            if not self._pending.resolve(
                pending.id, pending.version, PendingActionStatus.CONFIRMED,
                msg.message_id, now=datetime.now()
            ):
                return self._complete(item, msg, "REJECTED")
            result = self._executor.execute(
                cmd, message=msg, pending_action_id=pending.id, pending_version=pending.version
            )
            if result.status == RESULT_OK:
                self._notifier.flush()
            return self._complete(item, msg, "EXECUTED")

        target_ids = pending.candidate_ticket_ids
        target_id = target_ids[0] if len(target_ids) == 1 else None
        if target_id is None:
            return self._create_clarify_pending_from(
                item, msg, pending.intent, pending.fields, pending.candidate_ticket_ids,
                "确认前请明确工单编号",
            )
        ticket = self._db.get_ticket(target_id)
        if ticket is None:
            return self._complete(item, msg, "REJECTED")
        # 版本冲突 → 重新确认
        expected = pending.expected_ticket_versions.get(target_id)
        if expected is not None and ticket["version"] != expected:
            self._notifier.send_group_now(
                msg.group_id, "该工单状态已更新，请重新确认后再操作。", message_id=msg.message_id
            )
            return self._complete(item, msg, "REJECTED")
        if not self._pending.resolve(
            pending.id, pending.version, PendingActionStatus.CONFIRMED, msg.message_id, now=datetime.now()
        ):
            return self._complete(item, msg, "REJECTED")

        cmd = _command_from_pending(msg, pending, target_id)
        result = self._executor.execute(
            cmd, message=msg, pending_action_id=pending.id, pending_version=pending.version
        )
        if result.status == RESULT_OK:
            self._notifier.flush()
        return self._complete(item, msg, "EXECUTED")

    def _reject_pending(self, item: dict[str, Any], msg: NormalizedMessage, pending: Any) -> str:
        self._pending.resolve(
            pending.id, pending.version, PendingActionStatus.REJECTED, msg.message_id, now=datetime.now()
        )
        self._notifier.send_group_now(msg.group_id, "已取消该操作。", message_id=msg.message_id)
        return self._complete(item, msg, "REJECTED")

    def _correct_pending(
        self, item: dict[str, Any], msg: NormalizedMessage, pending: Any, decision: SemanticDecision
    ) -> str:
        """修正待确认动作字段后重新执行。"""
        merged = dict(pending.fields)
        merged.update(decision.fields)
        target_ids = pending.candidate_ticket_ids
        target_id = target_ids[0] if len(target_ids) == 1 else None
        if target_id is None:
            return self._create_clarify_pending_from(
                item, msg, pending.intent, merged, pending.candidate_ticket_ids, "请明确工单编号",
            )
        self._pending.resolve(
            pending.id, pending.version, PendingActionStatus.CONFIRMED, msg.message_id, now=datetime.now()
        )
        cmd = _command_from_pending(msg, pending, target_id, fields=merged)
        result = self._executor.execute(
            cmd, message=msg, pending_action_id=pending.id, pending_version=pending.version
        )
        if result.status == RESULT_OK:
            self._notifier.flush()
        return self._complete(item, msg, "EXECUTED")

    # ─────────────────────── 澄清/确认待办 ───────────────────────
    def _create_clarify_pending(
        self,
        item: dict[str, Any],
        msg: NormalizedMessage,
        decision: SemanticDecision,
        candidates: list[Any],
        route: Any,
    ) -> str:
        ids = tuple(c.ticket_id for c in candidates)
        if not ids:
            text = "当前没有可操作的活动工单，请先创建工单。"
        else:
            lines = "\n".join(f"{i + 1}. {c.ticket_no}（{c.subject}）" for i, c in enumerate(candidates))
            text = f"消息对应多张工单，请选择或提供编号：\n{lines}"
        self._notifier.send_group_now(msg.group_id, text, message_id=msg.message_id)
        return self._complete(item, msg, "CLARIFY")

    def _create_clarify_pending_from(
        self, item: dict[str, Any], msg: NormalizedMessage, intent: str, fields: dict[str, Any],
        candidate_ids: tuple[int, ...], text: str,
    ) -> str:
        self._notifier.send_group_now(msg.group_id, text, message_id=msg.message_id)
        return self._complete(item, msg, "CLARIFY")

    def _create_confirm_pending(
        self,
        item: dict[str, Any],
        msg: NormalizedMessage,
        decision: SemanticDecision,
        cmd: Any,
        route: Any,
        validate_candidates: list[Any],
    ) -> str:
        """协议确认策略 / 辅助模式：创建待确认动作。"""
        target_id = cmd.target_ticket_id if cmd is not None else route.target_ticket_id
        target = next((c for c in validate_candidates if c.ticket_id == target_id), None)
        versions = {target.ticket_id: target.version} if target else {}
        draft = PendingActionDraft(
            source_message_id=msg.message_id,
            group_id=msg.group_id,
            user_id=msg.sender_id,
            decision=decision,
            expected_ticket_versions=versions,
            expires_at=datetime.now(),
        )
        self._pending.create_or_supersede(draft, datetime.now())
        ticket_no = decision.target_ticket_no or (target.ticket_no if target else "该工单")
        self._notifier.send_group_now(
            msg.group_id, f"确认执行：{decision.intent}（{ticket_no}）？回复「确认」继续。",
            message_id=msg.message_id,
        )
        return self._complete(item, msg, "WAITING_CONFIRMATION")

    def _handle_select(
        self, item: dict[str, Any], msg: NormalizedMessage, decision: SemanticDecision, candidates: list[Any]
    ) -> str:
        target = None
        if decision.target_ticket_no:
            target = next((c for c in candidates if c.ticket_no == decision.target_ticket_no), None)
        elif len(candidates) == 1:
            target = candidates[0]
        if target is None:
            self._notifier.send_group_now(
                msg.group_id, "没有找到该工单，请检查编号。", message_id=msg.message_id
            )
            return self._complete(item, msg, "REJECTED")
        self._context.select(
            msg.group_id, msg.sender_id, target.ticket_id,
            order_key=f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}|{msg.message_id}",
            now=datetime.now(),
        )
        self._notifier.send_group_now(
            msg.group_id, f"已切换到工单 {target.ticket_no}。", message_id=msg.message_id
        )
        return self._complete(item, msg, "EXECUTED")

    def _handle_query(
        self, item: dict[str, Any], msg: NormalizedMessage, decision: SemanticDecision, candidates: list[Any]
    ) -> str:
        """查询工单：无编号时列出当前活动工单。"""
        if decision.target_ticket_no:
            target = next((c for c in candidates if c.ticket_no == decision.target_ticket_no), None)
            if target is None:
                self._notifier.send_group_now(
                    msg.group_id, "没有找到该工单，请检查编号。", message_id=msg.message_id
                )
                return self._complete(item, msg, "REJECTED")
            ticket = self._db.get_ticket(target.ticket_id)
            text = (
                f"📋 {ticket['ticket_no']}  {ticket['status']}\n"
                f"主题：{ticket['subject']}\n位置：{ticket['location']}\n"
                f"问题：{ticket['problem_description'][:80]}\n"
                f"预计完成：{ticket['current_deadline_at']}"
            )
            self._notifier.send_group_now(msg.group_id, text, message_id=msg.message_id)
            return self._complete(item, msg, "EXECUTED")

        if not candidates:
            self._notifier.send_group_now(
                msg.group_id, "当前没有活动工单。", message_id=msg.message_id
            )
            return self._complete(item, msg, "EXECUTED")

        lines = "\n".join(
            f"- {c.ticket_no}：{c.subject} @ {c.location}（{c.status}）" for c in candidates
        )
        self._notifier.send_group_now(
            msg.group_id, f"当前活动工单：\n{lines}", message_id=msg.message_id
        )
        return self._complete(item, msg, "EXECUTED")

    # ─────────────────────── 收尾 ───────────────────────
    def _quoted_ticket_id(self, msg: NormalizedMessage) -> int | None:
        if not msg.reply_to_message_id:
            return None
        return self._db.get_quoted_ticket_id(msg.reply_to_message_id)

    def _retry_or_dead(self, item: dict[str, Any], msg: NormalizedMessage, error: str) -> str:
        attempts = (item.get("attempts") or 0) + 1
        if attempts >= self._max_attempts:
            self._db.inbox_set_status(
                msg.message_id, _INBOX_DEAD, last_error=error, attempts=attempts
            )
            self._notifier.send_group_now(
                msg.group_id, "智能识别暂时不可用，本条消息未执行，请使用标准关键词或稍后重试。",
                message_id=msg.message_id,
            )
            return _INBOX_DEAD
        delay = self._retry_delays[min(attempts - 1, len(self._retry_delays) - 1)]
        next_at = _add_seconds(datetime.now(), delay)
        self._db.inbox_set_status(
            msg.message_id, _INBOX_RETRY, attempts=attempts,
            last_error=error, next_attempt_at=next_at,
        )
        logger.info("模型失败，进入重试 message_id=%s attempt=%d/%d delay=%ss",
                    msg.message_id, attempts, self._max_attempts, delay)
        return _INBOX_RETRY

    def _reject(self, item: dict[str, Any], msg: NormalizedMessage, errors: tuple[str, ...]) -> str:
        self._db.inbox_set_status(msg.message_id, _INBOX_COMPLETED, processed_result="REJECTED")
        self._db.record_processed_event(msg.message_id, msg.group_id, "REJECTED")
        text = "无法执行：" + "；".join(errors[:3])
        self._notifier.send_group_now(msg.group_id, text, message_id=msg.message_id)
        return _INBOX_COMPLETED

    def _complete(self, item: dict[str, Any] | None, msg: NormalizedMessage | None, result: str) -> str:
        if item is not None:
            self._db.inbox_set_status(item["message_id"], _INBOX_COMPLETED, processed_result=result)
        # 除影子模式外，所有终态都记幂等台账（防同 message_id 重复处理）
        if msg is not None and result != "SHADOW":
            self._db.record_processed_event(msg.message_id, msg.group_id, result)
        return _INBOX_COMPLETED


# ─────────────────────── 工具 ───────────────────────

# 裸订单号识别：消息基本就是一个淘宝订单号（可能带「单号/订单号」前缀）
_ORDER_PREFIXES = ("淘宝订单号", "订单号", "快递单号", "物流单号", "单号")
_ORDER_NO_RE = re.compile(r"^[A-Za-z0-9-]{6,64}$")
_STRIP_PUNCT = "，。！？、；：…,.;:!? "


def _extract_bare_order_no(content: str) -> str | None:
    """消息内容几乎只含一个订单号时返回它，否则返回 None。"""
    text = content.strip().strip(_STRIP_PUNCT)
    for prefix in _ORDER_PREFIXES:
        if text.startswith(prefix):
            text = text[len(prefix):].strip().strip(_STRIP_PUNCT)
            break
    if not text or not _ORDER_NO_RE.match(text):
        return None
    # 至少含 6 个数字，避免把短文本/编号误判成订单号
    digits = sum(ch.isdigit() for ch in text)
    return text if digits >= 6 else None


def _row_to_message(row: dict[str, Any]) -> NormalizedMessage:
    return NormalizedMessage(
        message_id=row["message_id"],
        group_id=row["group_id"],
        sender_id=row["sender_id"],
        sender_name=row.get("sender_id", ""),
        content=row.get("content", ""),
        message_type=row.get("message_type", "text"),
        sent_at=parse_naive_dt(row.get("sent_at", "")) or datetime.now(),
        sender_role=row.get("sender_role", "UNKNOWN"),
        reply_to_message_id=row.get("reply_to_message_id"),
    )


def _command_from_pending(msg: NormalizedMessage, pending: Any, target_id: int,
                          fields: dict[str, Any] | None = None) -> Any:
    from semantics.types import ValidatedCommand

    return ValidatedCommand(
        message_id=msg.message_id,
        group_id=msg.group_id,
        actor_id=msg.sender_id,
        actor_role=msg.sender_role,
        intent=pending.intent,
        target_ticket_id=target_id,
        expected_ticket_version=pending.expected_ticket_versions.get(target_id),
        fields=dict(fields if fields is not None else pending.fields),
        source="model",
    )


def _add_seconds(now: datetime, seconds: float) -> str:
    from datetime import timedelta

    return (now + timedelta(seconds=seconds)).strftime("%Y-%m-%d %H:%M:%S")
