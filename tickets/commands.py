"""受控命令定义与回复文案（计划书 §11.8、Task 9）。

执行器映射必须来自 ALLOWED_EXECUTORS，使用静态字典，不使用 eval / 反射。
"""

from __future__ import annotations

from typing import Any

# 协议允许的执行器（与 protocol_loader 校验一致）
ALLOWED_EXECUTORS = frozenset({
    "create_ticket", "add_ticket_detail", "submit_diagnosis",
    "submit_repair_plan", "submit_timeout_reason", "complete_ticket",
    "cancel_ticket", "reopen_ticket", "query_ticket", "select_ticket",
    "clarify", "confirm_pending_action", "reject_pending_action",
    "correct_pending_action", "ignore_message",
})


def reply_text(intent: str, ticket: dict[str, Any] | None, fields: dict[str, Any]) -> str:
    """根据意图与执行后的工单生成群内回复。"""
    ticket_no = ticket["ticket_no"] if ticket else ""
    if intent == "ticket.create" and ticket:
        return (
            f"✅ 已创建工单：{ticket_no}\n"
            f"主题：{ticket['subject']}\n"
            f"位置：{ticket['location']}\n"
            f"预计完成：{ticket['current_deadline_at']}"
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
            f"预计完成：{ticket['current_deadline_at']}"
        )
    return "已处理"
