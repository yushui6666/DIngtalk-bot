"""语义分类器（计划书 Task 4 §10）。

SemanticClassifier 负责：
1. 跳过已被关键词匹配器命中的消息（不浪费 API 调用）；
2. 组装协议驱动的提示词（动作子集 + 候选工单摘要 + 固定 Schema）；
3. 调用 ModelClient.complete_json（单次 HTTP）；
4. 过滤模型幻觉字段 + 校验枚举值；
5. 生成 SemanticDecision（失败时降级为 chat.ignore）。

安全要求（§10.4 提示注入防护）：
- 用户消息作为不可信数据字段传入，不做指令拼接；
- 模型输出只接受协议内 intent 和白名单字段；
- 本地重新校验，模型不能创造新动作/字段/状态。
"""

from __future__ import annotations

from typing import Any

from semantics.protocol_loader import TicketProtocol
from semantics.types import SemanticDecision, TicketCandidate, TicketScore, PendingAction
from semantics.model_client import ModelTimeoutError, ModelResponseError
from semantics.keyword_matcher import match_keyword

from models import NormalizedMessage

from logger import get_logger

logger = get_logger(__name__)

# ───────────────────────── 字段白名单 ─────────────────────────
# 计划书 §4.5 字段词典：模型只能抽取这些键，其余一律视为幻觉丢弃
_KNOWN_FIELDS: frozenset[str] = frozenset({
    "subject", "location", "problem_description", "device", "urgency",
    "sla", "diagnosis_items", "repair_method", "order_no",
    "timeout_reason", "completion_note", "cancel_reason", "reopen_reason",
    "clarification_reason", "ticket_no", "content", "attachments",
})

_INTENT_FIELD_OVERRIDES: dict[str, frozenset[str]] = {
    # sla 已改为可选（默认 1 天），但模型仍可抽取，不能当幻觉过滤
    "ticket.create": frozenset({"device", "urgency", "attachments", "sla"}),
    "ticket.add_detail": frozenset({
        "subject", "location", "problem_description", "device", "urgency",
        "attachments", "content",
    }),
    "ticket.diagnosis.submit": frozenset({"diagnosis_items"}),
    "ticket.repair_plan.submit": frozenset({"repair_method", "order_no"}),
    "ticket.timeout_reason.submit": frozenset({"timeout_reason"}),
    "ticket.complete": frozenset({"completion_note"}),
    "system.correct_pending_action": _KNOWN_FIELDS - {"ticket_no"},
}

# 允许的 SLA 枚举值（§4.5）
_ALLOWED_SLA = frozenset({"1天", "3天", "7天"})

# 允许的紧急度枚举值（§4.5）
_ALLOWED_URGENCY = frozenset({"低", "中", "高"})

# 允许的维修方式枚举值（§4.7）
_ALLOWED_REPAIR_METHODS = frozenset({
    "淘宝采购后自行维修",
    "需要供应商维修",
    "需要木工维修",
    "需要工程师上门",
    "远程视频维修",
})

_BUSINESS_ACTION_CUES: dict[str, tuple[str, ...]] = {
    "ticket.create": ("报修",),
    "ticket.diagnosis.submit": ("故障判断", "判断是", "应该是"),
    "ticket.repair_plan.submit": tuple(_ALLOWED_REPAIR_METHODS),
    "ticket.timeout_reason.submit": ("超时原因", "没按时完成"),
    "ticket.complete": ("完成工单", "直接完毕", "确认完毕"),
    "ticket.cancel": ("取消工单",),
    "ticket.reopen": ("重开工单",),
}


class SemanticClassifier:
    """云端模型语义分类器。

    协议驱动的提示词组装 + 模型输出后处理（字段裁剪、枚举校验）。

    设计原则（§2.2）：
    - 模型只做理解和抽取，不做执行；
    - 本地规则引擎始终重新校验。
    """

    def __init__(
        self,
        *,
        client: Any,  # ModelClient Protocol（types.py）
        protocol: TicketProtocol,
    ) -> None:
        self._client = client
        self._protocol = protocol

    async def classify(
        self,
        message: NormalizedMessage,
        candidates: list[TicketCandidate] | None = None,
        pending_action: PendingAction | None = None,
        history: list[dict[str, Any]] | None = None,
    ) -> SemanticDecision:
        """对自然语言消息做语义分类。

        流程（§7 消息处理流程）：
        1. 关键词已命中 → 跳过模型（source 标记 keyword_already_matched）；
        2. 组装 prompt + schema（含最近群消息上下文）；
        3. 单次 HTTP 调用模型；
        4. 校验 intent 在协议内；
        5. 过滤幻觉字段 + 枚举值校验；
        6. 返回 SemanticDecision。

        Args:
            message: 标准化群消息。
            candidates: 当前群的活动工单候选列表（多工单路由）。
            pending_action: 当前待确认动作（用于上下文）。
            history: 该群最近消息（时间正序），供模型理解「2」「005完成了」等
                依赖上文的表达。
        """
        candidates = candidates or []

        # 1. 关键词已命中 → 跳过模型（§2.3 快路径优先）
        if match_keyword(message.content, self._protocol) is not None:
            return SemanticDecision(
                protocol_version=self._protocol.protocol_version,
                source="SEMANTIC_MODEL",
                intent="chat.ignore",
                target_ticket_no=None,
                intent_confidence=0.0,
                evidence=("keyword_already_matched",),
            )

        action_cues = _find_business_action_cues(message.content)
        if len(action_cues) > 1:
            evidence = tuple(
                cue
                for _, cues in action_cues
                for cue in cues
            )
            return SemanticDecision(
                protocol_version=self._protocol.protocol_version,
                source="SEMANTIC_MODEL",
                intent="system.clarify",
                target_ticket_no=None,
                intent_confidence=1.0,
                fields={
                    "clarification_reason": "同一消息包含多个业务动作，请拆分后重发",
                },
                evidence=evidence,
            )

        # 2. 组装 prompt 和 schema
        payload = _build_payload(message, candidates, pending_action, self._protocol, history=history)
        schema = _build_output_schema(self._protocol)

        # 3. 调用模型（单次，不内部重试）
        try:
            raw = await self._client.complete_json(
                payload=payload,
                schema=schema,
                idempotency_key=f"classify:{message.message_id}",
            )
        except ModelTimeoutError as exc:
            logger.warning("模型超时 message_id=%s err=%s", message.message_id, exc)
            return _fallback_decision(self._protocol, message.message_id, "timeout")
        except ModelResponseError as exc:
            logger.warning("模型响应异常 message_id=%s err=%s", message.message_id, exc)
            return _fallback_decision(self._protocol, message.message_id, "response_error")
        except Exception as exc:
            # 网络错误等未知异常
            logger.warning("模型调用失败 message_id=%s err=%s", message.message_id, exc)
            return _fallback_decision(self._protocol, message.message_id, "network_error")

        if not isinstance(raw, dict):
            logger.warning(
                "模型响应不是 JSON 对象 message_id=%s type=%s",
                message.message_id,
                type(raw).__name__,
            )
            return _fallback_decision(self._protocol, message.message_id, "response_error")

        # 4. 校验 intent 在协议白名单内（§10.4 提示注入防护）
        intent = raw.get("intent", "")
        known_intents = {a.intent_id for a in self._protocol.actions}
        if intent not in known_intents:
            logger.warning(
                "模型返回协议外 intent=%s message_id=%s（提示注入？）",
                intent,
                message.message_id,
            )
            return _fallback_decision(self._protocol, message.message_id, "unknown_intent")

        action = self._protocol.get_action(intent)
        if action is None:
            return _fallback_decision(self._protocol, message.message_id, "unknown_intent")

        # 5. 过滤幻觉字段（§10.4：模型不能创造新字段）
        raw_fields = raw.get("fields", {}) or {}
        if not isinstance(raw_fields, dict):
            return _fallback_decision(self._protocol, message.message_id, "response_error")
        allowed_fields = (
            set(action.required_fields)
            | set(action.optional_fields)
            | set(_INTENT_FIELD_OVERRIDES.get(intent, ()))
        )
        safe_fields: dict[str, Any] = {}
        for key, value in raw_fields.items():
            if key == "ticket_no" or key in allowed_fields:
                safe_fields[key] = value
            else:
                logger.debug(
                    "过滤幻觉字段 key=%s message_id=%s（提示注入？）",
                    key,
                    message.message_id,
                )

        # 6. 枚举值校验（§4.5, §4.7）
        missing: list[str] = []

        if "sla" in safe_fields and safe_fields["sla"] not in _ALLOWED_SLA:
            logger.debug("sla 非法值=%s，移入 missing", safe_fields["sla"])
            missing.append("sla")
            del safe_fields["sla"]

        if "urgency" in safe_fields and safe_fields["urgency"] not in _ALLOWED_URGENCY:
            logger.debug("urgency 非法值=%s，移入 missing", safe_fields["urgency"])
            missing.append("urgency")
            del safe_fields["urgency"]

        if "repair_method" in safe_fields and safe_fields["repair_method"] not in _ALLOWED_REPAIR_METHODS:
            logger.debug("repair_method 非法值=%s，移入 missing", safe_fields["repair_method"])
            missing.append("repair_method")
            del safe_fields["repair_method"]

        # 置信度
        confidence = 0.0
        try:
            confidence = float(raw.get("confidence", 0.0))
            confidence = max(0.0, min(1.0, confidence))
        except (TypeError, ValueError):
            logger.debug("confidence 非法=%s，设为 0", raw.get("confidence"))

        # 提取工单编号
        ticket_no = safe_fields.pop("ticket_no", None) or raw.get("ticket_no")
        if ticket_no is not None:
            ticket_no = str(ticket_no).strip() or None
        candidate_nos = {candidate.ticket_no for candidate in candidates}
        if ticket_no and ticket_no not in candidate_nos:
            logger.debug(
                "过滤候选集合外目标 ticket_no=%s message_id=%s",
                ticket_no,
                message.message_id,
            )
            ticket_no = None
            missing.append("ticket_no")

        # 候选评分（模型可选返回）
        candidate_scores: tuple[TicketScore, ...] = ()
        raw_scores = raw.get("candidate_scores")
        if isinstance(raw_scores, list):
            valid_scores: list[TicketScore] = []
            for item in raw_scores:
                if isinstance(item, dict):
                    tn = item.get("ticket_no", "")
                    sc = item.get("score", 0.0)
                    # 只保留当前群候选集合内的工单（§6.2：模型不能选群外工单）
                    if tn in candidate_nos:
                        try:
                            valid_scores.append(TicketScore(ticket_no=tn, score=float(sc)))
                        except (TypeError, ValueError):
                            pass
            if valid_scores:
                candidate_scores = tuple(valid_scores)

        evidence_raw = raw.get("evidence", [])
        evidence: tuple[str, ...] = ()
        if isinstance(evidence_raw, list):
            evidence = tuple(str(e) for e in evidence_raw if e)

        return SemanticDecision(
            protocol_version=self._protocol.protocol_version,
            source="SEMANTIC_MODEL",
            intent=intent,
            target_ticket_no=ticket_no,
            intent_confidence=confidence,
            fields=safe_fields,
            missing_fields=tuple(missing),
            candidate_scores=candidate_scores,
            evidence=evidence,
        )


def _find_business_action_cues(
    content: str,
) -> list[tuple[str, tuple[str, ...]]]:
    """识别同一消息中明确出现的多个业务动作词。"""
    matches: list[tuple[str, tuple[str, ...]]] = []
    for intent, cues in _BUSINESS_ACTION_CUES.items():
        matched = tuple(cue for cue in cues if cue in content)
        if matched:
            matches.append((intent, matched))
    return matches


# ───────────────────────── 降级决策 ─────────────────────────


def _fallback_decision(
    protocol: TicketProtocol,
    message_id: str,
    reason: str,
) -> SemanticDecision:
    """模型失败时的降级决策。

    §10.3：模型不可用时自然语言进入重试或死信；
    显式关键词快路径不受影响。
    """
    return SemanticDecision(
        protocol_version=protocol.protocol_version,
        source="SEMANTIC_MODEL",
        intent="chat.ignore",
        target_ticket_no=None,
        intent_confidence=0.0,
        evidence=(f"model_fallback:{reason}:{message_id}",),
    )


# ───────────────────────── prompt 组装 ─────────────────────────


def _build_payload(
    message: NormalizedMessage,
    candidates: list[TicketCandidate],
    pending_action: PendingAction | None,
    protocol: TicketProtocol,
    history: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """组装模型请求 payload（§10.2 模型输入）。

    精简策略（2026-08-12 优化，降低模型延迟）：
    - 动作摘要去掉 display_name / 可选字段 / target_policy / confirmation_policy
      （分类不需要，本地校验兜底）；
    - 按发送人角色裁剪动作子集（system.* 无角色限制始终保留）；
    - 每个动作只保留必填字段 + 一条截断正例。

    上下文（2026-08-13）：附带最近群消息，帮助模型理解「2」「005完成了」等
    依赖上文的表达。
    """
    sender_role = message.sender_role
    action_summaries: list[str] = []
    for a in protocol.actions:
        if not a.semantic_enabled:
            continue
        # 角色裁剪：无角色限制（system.*）保留；否则只保留该角色可触发的动作
        if a.allowed_roles and sender_role not in a.allowed_roles:
            continue
        parts = [f"- {a.intent_id}"]
        if a.required_fields:
            parts.append("必填:" + ",".join(a.required_fields))
        example = next((e for e in a.positive_examples if e), "")
        if example:
            parts.append("例:" + example.replace("\n", " ")[:48])
        action_summaries.append(" ".join(parts))

    # 候选工单摘要（§6.2：只发编号/主题/位置/状态/进展）
    candidate_lines = ""
    if candidates:
        candidate_lines = "\n当前活动工单：\n"
        for c in candidates:
            candidate_lines += (
                f"  - {c.ticket_no}: {c.subject} @ {c.location} "
                f"summary={c.problem_summary} status={c.status} version={c.version}\n"
            )

    # 待确认动作上下文
    pending_line = ""
    if pending_action is not None:
        pending_line = (
            f"\n当前待确认动作: pending_id={pending_action.id} "
            f"intent={pending_action.intent} "
            f"candidates={pending_action.candidate_ticket_ids} "
            f"fields={pending_action.fields} version={pending_action.version}\n"
        )

    system_prompt = (
        "你是钉钉群报修工单的语义分析助手。"
        "根据用户消息判断意图(intent)和抽取字段(fields)。\n\n"
        f"发送人角色={sender_role}。协议版本={protocol.protocol_version}。\n"
        "可用的意图(id)：\n"
        + "\n".join(action_summaries)
        + "\n\n规则：\n"
        "- 只抽取用户明确表达的信息\n"
        "- 不要猜测位置、设备名称等\n"
        "- 字段键名使用英文字段名\n"
        "- 如果消息没有明确报修/诊断/完成等意图，返回 chat.ignore\n"
        "- 同一消息包含两个或更多业务动作时，即使后一个动作是未来或条件动作，也返回 system.clarify\n"
        "- 用户从候选工单中明确选择编号或序号时，返回 ticket.select\n"
        "- 工程师使用‘可能’‘应该是’等保留表达给出具体故障判断时，仍可返回 ticket.diagnosis.submit，但应降低置信度\n"
        "- 疑问句但明确描述故障（含设备/位置/问题）时，仍返回 ticket.create；纯笼统询问、否定句返回 chat.ignore\n"
        "- 用户表示问题已解决、恢复正常、可以使用（如「正常了」「搞定了」「弄好了」「没问题了」「修好了」）→ 返回 ticket.complete\n"
        "- 取消工单(ticket.cancel)、重开工单(ticket.reopen) 只在用户非常确定时才返回\n"
    )

    if candidate_lines:
        system_prompt += candidate_lines
    if pending_line:
        system_prompt += pending_line

    # 最近群消息上下文（供模型理解「2」「005完成了」「第二个」等依赖上文的表达）
    if history:
        history_lines = "\n最近群消息（供理解上下文，勿直接引用，当前消息是最后一句）：\n"
        for h in history:
            sender = h.get("sender_name") or h.get("sender_role") or h.get("sender_id") or "?"
            content = str(h.get("content") or "")[:120]
            history_lines += f"[{sender}] {content}\n"
        system_prompt += history_lines

    # 用户消息作为不可信数据字段（§10.4 提示注入防护）
    return {
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": message.content},
        ],
    }


# ───────────────────────── 输出 Schema ─────────────────────────


def _build_output_schema(protocol: TicketProtocol) -> dict[str, Any]:
    """生成结构化输出的 JSON Schema（§4.9 模型固定输出）。

    模型只能返回协议内 intent 和固定字段结构。
    """
    intents = [a.intent_id for a in protocol.actions if a.semantic_enabled]
    return {
        "type": "object",
        "properties": {
            "intent": {
                "type": "string",
                "enum": intents,
                "description": "用户消息对应的意图 ID",
            },
            "confidence": {
                "type": "number",
                "minimum": 0.0,
                "maximum": 1.0,
                "description": "置信度 [0.0, 1.0]",
            },
            "fields": {
                "type": "object",
                "description": "抽取的字段键值对",
                "properties": {
                    "subject": {"type": "string"},
                    "location": {"type": "string"},
                    "problem_description": {"type": "string"},
                    "device": {"type": "string"},
                    "urgency": {"type": "string", "enum": ["低", "中", "高"]},
                    "sla": {"type": "string", "enum": ["1天", "3天", "7天"]},
                    "diagnosis_items": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                    "repair_method": {
                        "type": "string",
                        "enum": sorted(_ALLOWED_REPAIR_METHODS),
                    },
                    "order_no": {"type": "string"},
                    "timeout_reason": {"type": "string"},
                    "completion_note": {"type": "string"},
                    "cancel_reason": {"type": "string"},
                    "reopen_reason": {"type": "string"},
                    "clarification_reason": {"type": "string"},
                    "ticket_no": {"type": "string"},
                    "content": {"type": "string"},
                },
            },
            "evidence": {
                "type": "array",
                "items": {"type": "string"},
                "description": "原文中支持判断的片段",
            },
            "ticket_no": {
                "type": "string",
                "description": "消息中明确提及的工单编号，无则省略",
            },
            "candidate_scores": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "ticket_no": {"type": "string"},
                        "score": {"type": "number"},
                    },
                },
                "description": "对候选工单的评分（可选）",
            },
        },
        "required": ["intent", "confidence", "fields"],
    }
