"""语义决策校验器（计划书 Task 2）。

validate_decision() 对 match_keyword 或 model_classifier 产出的
SemanticDecision 做二次校验（权限、必填字段、枚举值、订单号规则），
输出 (DecisionStatus, ValidatedCommand | None, errors)。

确定性校验——不调用模型，不访问数据库（candidates 由调用方注入）。
"""

from __future__ import annotations

import re
from typing import Any

from semantics.protocol_loader import TicketProtocol
from semantics.types import (
    DecisionStatus,
    SemanticDecision,
    TicketCandidate,
    ValidatedCommand,
)

from models import NormalizedMessage

# 订单号占位值（v3.0 遗留规则，仍适用）
_ORDER_NO_PLACEHOLDERS = frozenset({"无", "暂无", "稍后补", "不知道"})

# 订单号正则（去空白后 6-64 位字母数字连字符）
_ORDER_NO_PATTERN = re.compile(r"^[A-Za-z0-9-]{6,64}$")

# 五种允许维修方式
_ALLOWED_REPAIR_METHODS = frozenset({
    "淘宝采购后自行维修",
    "需要供应商维修",
    "需要木工维修",
    "需要工程师上门",
    "远程视频维修",
})

# SLA 允许值
_ALLOWED_SLA = frozenset({"1天", "3天", "7天"})


def _validate_role(action: Any, message: NormalizedMessage) -> bool:
    """校验用户角色是否在动作的允许角色列表中。"""
    if not action.allowed_roles:
        return True  # 无限制（如 system.* / chat.ignore）
    return message.sender_role in action.allowed_roles


def _validate_required_fields(
    action: Any,
    fields: dict[str, Any],
    target_ticket_no: str | None,
) -> list[str]:
    """校验必填字段是否存在且非空。"""
    missing: list[str] = []
    for fname in action.required_fields:
        if fname == "ticket_no":
            if not target_ticket_no:
                missing.append(fname)
            continue
        val = fields.get(fname)
        if val is None or (isinstance(val, str) and not val.strip()):
            missing.append(fname)
        elif isinstance(val, list) and not val:
            missing.append(fname)
    return missing


def _validate_enum(field_name: str, value: Any, action: Any) -> str | None:
    """校验枚举字段值是否合法。返回错误消息或 None。"""
    fd = action.field_definitions.get(field_name, {})
    allowed = fd.get("allowed")
    if allowed and isinstance(allowed, list):
        if value not in allowed:
            return f"'{field_name}' 值 '{value}' 不在允许范围 {allowed}"
    return None


def _validate_order_no(fields: dict[str, Any]) -> str | None:
    """淘宝采购维修方式时校验订单号。"""
    repair_method = fields.get("repair_method", "")
    if repair_method != "淘宝采购后自行维修":
        return None

    order_no = fields.get("order_no", "")
    if not order_no:
        return "选择'淘宝采购后自行维修'必须提供订单号"

    # 去除空白
    cleaned = "".join(order_no.split())
    if not cleaned:
        return "订单号不能为空"

    if cleaned in _ORDER_NO_PLACEHOLDERS:
        return f"订单号不能使用占位值 '{order_no}'"

    if not _ORDER_NO_PATTERN.match(cleaned):
        return f"订单号 '{order_no}' 格式不合法（需要 6-64 位字母数字连字符）"

    return None


def _dispatch_reason_field(fields: dict[str, Any], intent: str) -> None:
    """将通用 '原因' 字段按 intent 分发到具体字段名。

    #取消工单 → cancel_reason
    #重开工单 → reopen_reason
    """
    if "reason" not in fields:
        return
    reason = fields.pop("reason")
    if intent == "ticket.cancel":
        fields["cancel_reason"] = reason
    elif intent == "ticket.reopen":
        fields["reopen_reason"] = reason
    # 其他 intent 不需要 reason 字段


def _build_validated_command(
    decision: SemanticDecision,
    message: NormalizedMessage,
    target_ticket_id: int | None,
    expected_ticket_version: int | None,
    fields: dict[str, Any],
) -> ValidatedCommand:
    return ValidatedCommand(
        message_id=message.message_id,
        group_id=message.group_id,
        actor_id=message.sender_id,
        actor_role=message.sender_role,
        intent=decision.intent,
        target_ticket_id=target_ticket_id,
        expected_ticket_version=expected_ticket_version,
        fields=fields,
        source=decision.source,
    )


def _resolve_target_candidate(
    decision: SemanticDecision,
    action: Any,
    message: NormalizedMessage,
    candidates: list[TicketCandidate],
) -> tuple[TicketCandidate | None, list[str]]:
    """按协议目标策略解析当前群内候选工单。"""
    errors: list[str] = []
    group_candidates = [candidate for candidate in candidates if candidate.group_id == message.group_id]
    target: TicketCandidate | None = None

    if decision.target_ticket_no:
        target = next(
            (
                candidate
                for candidate in group_candidates
                if candidate.ticket_no == decision.target_ticket_no
            ),
            None,
        )
        if target is None:
            errors.append(f"目标工单 '{decision.target_ticket_no}' 不存在或不属于当前群")
    elif action.target_ticket_policy == "MUST_EXIST":
        if len(group_candidates) == 1:
            target = group_candidates[0]
        elif not group_candidates:
            errors.append("该动作需要目标工单，但当前没有可用候选")
        else:
            errors.append("该动作需要唯一目标工单，请明确提供工单编号")

    if action.target_ticket_policy == "MUST_NOT_EXIST" and decision.target_ticket_no:
        errors.append("该动作不得绑定既有目标工单")

    if target and action.allowed_ticket_states and target.status not in action.allowed_ticket_states:
        errors.append(
            f"目标工单状态 '{target.status}' 不允许执行 '{decision.intent}'，"
            f"允许状态: {action.allowed_ticket_states}"
        )

    return target, errors


def _requires_confirmation(decision: SemanticDecision, action: Any) -> bool:
    """根据决策来源和协议确认策略判断是否进入确认流程。"""
    if decision.requires_confirmation:
        return True
    source_key = "EXPLICIT_KEYWORD" if decision.source == "keyword" else "SEMANTIC_MODEL"
    return action.confirmation_policy.get(source_key) == "ALWAYS"


def validate_decision(
    decision: SemanticDecision,
    *,
    message: NormalizedMessage,
    candidates: list[TicketCandidate],
    protocol: TicketProtocol,
) -> tuple[DecisionStatus, ValidatedCommand | None, tuple[str, ...]]:
    """校验语义决策，返回 (状态, 命令, 错误列表)。

    校验顺序：intent 查找 → ignore 快路径 → 角色权限 → 必填字段 →
    枚举值 → 订单号规则。

    candidates 是路由层冻结的当前群候选快照，用于目标、状态和版本校验。
    """
    action = protocol.get_action(decision.intent)
    if action is None:
        return (
            DecisionStatus.VALIDATION_REJECTED,
            None,
            (f"未知 intent_id: {decision.intent}",),
        )

    # chat.ignore 快路径
    if decision.intent == "chat.ignore":
        return (DecisionStatus.IGNORE, None, ())

    # system.* 校验（非 ignore 的系统动作）
    if decision.intent.startswith("system.") and decision.intent != "system.clarify":
        return (DecisionStatus.IGNORE, None, ())

    # system.clarify → 直接返回等待确认
    if decision.intent == "system.clarify":
        return (DecisionStatus.WAITING_CONFIRMATION, None, decision.missing_fields)

    fields = dict(decision.fields)

    # 字段分发：通用 "原因" → 按 intent 映射到具体字段
    _dispatch_reason_field(fields, decision.intent)

    errors: list[str] = []

    # 1. 角色权限
    if not _validate_role(action, message):
        from tickets.commands import intent_label

        errors.append(
            f"角色 '{message.sender_role}' 无权限执行「{intent_label(decision.intent)}」，"
            f"允许角色: {action.allowed_roles}"
        )

    # 2. 必填字段
    missing = _validate_required_fields(action, fields, decision.target_ticket_no)
    for mf in missing:
        errors.append(f"缺少必填字段 '{mf}'")

    # 3. 枚举值校验
    for fname, fvalue in fields.items():
        err = _validate_enum(fname, fvalue, action)
        if err:
            errors.append(err)

    # SLA 特殊枚举校验（字段名可能在不同 action 中）
    if "sla" in fields and fields["sla"] not in _ALLOWED_SLA:
        if "sla" not in [f for f in errors if "sla" in f]:
            errors.append(f"时效 '{fields['sla']}' 不在允许范围 ['1天', '3天', '7天']")

    # 维修方式枚举校验
    if "repair_method" in fields:
        if fields["repair_method"] not in _ALLOWED_REPAIR_METHODS:
            if "repair_method" not in [f for f in errors if "repair_method" in f]:
                errors.append(f"维修方式 '{fields['repair_method']}' 不在允许的 5 种方式中")

    # 4. 订单号校验（淘宝采购特例）
    if fields.get("repair_method") == "淘宝采购后自行维修":
        order_err = _validate_order_no(fields)
        if order_err:
            errors.append(order_err)

    # 5. 目标工单、群归属和状态校验
    target, target_errors = _resolve_target_candidate(decision, action, message, candidates)
    errors.extend(target_errors)

    if errors:
        return (DecisionStatus.VALIDATION_REJECTED, None, tuple(errors))

    if _requires_confirmation(decision, action):
        return (DecisionStatus.WAITING_CONFIRMATION, None, ())

    # 全部通过
    cmd = _build_validated_command(
        decision,
        message,
        target.ticket_id if target else None,
        target.version if target else None,
        fields,
    )
    return (DecisionStatus.AUTO_EXECUTE, cmd, ())
