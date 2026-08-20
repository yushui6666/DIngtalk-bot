"""受控命令定义与回复文案（计划书 §11.8、Task 9）。

执行器映射必须来自 ALLOWED_EXECUTORS，使用静态字典，不使用 eval / 反射。
"""

from __future__ import annotations

from typing import Any

# 协议允许的执行器（与 protocol_loader 校验一致）
ALLOWED_EXECUTORS = frozenset({
    "create_ticket", "add_ticket_detail", "submit_diagnosis",
    "submit_repair_plan", "submit_timeout_reason", "complete_ticket",
    "cancel_ticket", "stop_ticket", "reopen_ticket", "query_ticket", "select_ticket",
    "clarify", "confirm_pending_action", "reject_pending_action",
    "correct_pending_action", "ignore_message",
})

# 意图 → 中文可读文案（用户看到的提示/回执用，避免出现 system.clarify 这类英文 ID）
INTENT_LABELS: dict[str, str] = {
    "ticket.create": "报修建单",
    "ticket.add_detail": "补充工单信息",
    "ticket.diagnosis.submit": "记录故障判断",
    "ticket.repair_plan.submit": "提交维修方式/订单号",
    "ticket.timeout_reason.submit": "提交超时原因",
    "ticket.complete": "完成工单",
    "ticket.cancel": "取消工单",
    "ticket.stop": "停修工单",
    "ticket.reopen": "重开工单",
    "ticket.query": "查询工单",
    "ticket.select": "选择工单",
    "system.clarify": "需要您澄清",
    "system.confirm_pending_action": "确认待办",
    "system.reject_pending_action": "拒绝待办",
    "system.correct_pending_action": "修正待办",
    "chat.ignore": "忽略（闲聊）",
}


def intent_label(intent: str) -> str:
    """意图 ID → 中文文案；未映射时原样返回。"""
    return INTENT_LABELS.get(intent, intent)


def _deadline_text(ticket: dict[str, Any]) -> str:
    """工单截止文案；待商榷工单（无 deadline）显示「暂不设时效」。"""
    deadline = ticket.get("current_deadline_at")
    return f"预计完成：{deadline}" if deadline else "时效：待商榷（暂不设截止时间）"


def reply_text(intent: str, ticket: dict[str, Any] | None, fields: dict[str, Any]) -> str:
    """根据意图与执行后的工单生成群内回复。"""
    ticket_no = ticket["ticket_no"] if ticket else ""
    if intent == "ticket.create" and ticket:
        return (
            f"✅ 已创建工单：{ticket_no}\n"
            f"主题：{ticket['subject']}\n"
            f"位置：{ticket['location']}\n"
            f"{_deadline_text(ticket)}"
        )
    if intent == "ticket.add_detail":
        return f"已补充工单 {ticket_no} 的信息"
    if intent == "ticket.diagnosis.submit":
        return f"已记录故障判断 → {ticket_no}"
    if intent == "ticket.repair_plan.submit":
        return f"已记录维修方式 → {ticket_no}"
    if intent == "ticket.timeout_reason.submit":
        return f"已记录超时原因 → {ticket_no}"
    if intent == "ticket.complete":
        return f"工单 {ticket_no} 已完成 ✅"
    if intent == "ticket.cancel":
        return f"工单 {ticket_no} 已取消"
    if intent == "ticket.stop":
        return f"工单 {ticket_no} 已停修（不再维修）"
    if intent == "ticket.reopen":
        return f"工单 {ticket_no} 已重新开启"
    if intent == "ticket.select":
        return f"已切换到工单 {ticket_no}"
    if intent == "ticket.query" and ticket:
        return (
            f"📋 {ticket_no}  {ticket['status']}\n"
            f"主题：{ticket['subject']}\n"
            f"位置：{ticket['location']}\n"
            f"问题：{ticket['problem_description'][:80]}\n"
            f"{_deadline_text(ticket)}"
        )
    return "已处理"
