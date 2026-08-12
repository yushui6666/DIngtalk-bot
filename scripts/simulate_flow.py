"""整套业务流程模拟（2026-08-12 新流程）。

用假分类器模拟云端模型的语义判断，驱动真实 pipeline，打印每一步：
消息 → 决策 → 动作 → 数据库变化 → 群回执。

用法::

    python scripts/simulate_flow.py

流程覆盖：报修(多单并行)→选单→诊断→维修方式+订单号→快递确认→补充→完工→取消(需确认)→查询。
"""

from __future__ import annotations

import asyncio
import sys
from datetime import datetime
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from db import Database  # noqa: E402
from models import NormalizedMessage  # noqa: E402
from notifier import Notifier  # noqa: E402
from pipeline import MessageProcessingPipeline, RuntimeMode  # noqa: E402
from routing.delivery import DeliveryConfirmService  # noqa: E402
from routing.pending_actions import PendingActionService  # noqa: E402
from routing.ticket_contexts import TicketContextStore  # noqa: E402
from routing.ticket_router import TicketRouter  # noqa: E402
from semantics.protocol_loader import load_protocol  # noqa: E402
from semantics.types import SemanticDecision  # noqa: E402
from tickets.executor import TicketCommandExecutor  # noqa: E402
from tickets.repository import TicketRepository  # noqa: E402

GROUP = {"group_id": "G1", "store_name": "钉钉消息测试",
         "manager_ids": ["uid-mgr"], "engineer_ids": ["uid-eng"], "other_member_ids": ["uid-staff"]}

MG = ("uid-mgr", "MANAGER", "店长")   # 店长 / 报修人
EG = ("uid-eng", "ENGINEER", "工程师")
OT = ("uid-staff", "OTHER", "店员")


class FakeClassifier:
    """按 message_id 返回预设语义决策，模拟云端模型。"""

    def __init__(self) -> None:
        self.responses: dict[str, SemanticDecision] = {}

    async def classify(self, message, candidates=None, pending_action=None) -> SemanticDecision:
        return self.responses.get(
            message.message_id,
            SemanticDecision(protocol_version="4.0.0", source="SEMANTIC_MODEL",
                             intent="chat.ignore", target_ticket_no=None, intent_confidence=0.0),
        )


def _decision(intent, fields=None, ticket_no=None, confidence=0.95):
    return SemanticDecision(
        protocol_version="4.0.0", source="SEMANTIC_MODEL", intent=intent,
        target_ticket_no=ticket_no, intent_confidence=confidence,
        fields=fields or {},
    )


async def main() -> None:
    db = Database(_PROJECT_ROOT / "data" / "sim.db")
    if db.db_path.exists():
        db.db_path.unlink()
    db.init_schema()
    db.upsert_group(GROUP)

    protocol = load_protocol(_PROJECT_ROOT / "protocols" / "ticket_semantics.v4.json")
    repo = TicketRepository(db)
    router = TicketRouter()
    context = TicketContextStore(db)
    pending = PendingActionService(db)
    delivery = DeliveryConfirmService(db)
    executor = TicketCommandExecutor(db, repo)
    replies: list[str] = []
    notifier = Notifier(db, lambda target, text: replies.append("[群回执] " + text.replace("\n", " ⏎ ")))
    classifier = FakeClassifier()
    pipeline = MessageProcessingPipeline(
        db=db, repo=repo, protocol=protocol, router=router, context=context,
        pending=pending, executor=executor, notifier=notifier,
        classifier=classifier, delivery=delivery, mode=RuntimeMode.PRODUCTION,
    )

    def msg(message_id, text, actor):
        sender_id, role, name = actor
        return NormalizedMessage(
            message_id=message_id, group_id="G1", sender_id=sender_id, sender_name=name,
            content=text, message_type="text", sent_at=datetime.now(), sender_role=role,
        )

    async def send(message_id, text, actor, preset=None):
        if preset is not None:
            classifier.responses[message_id] = preset
        replies.clear()
        m = msg(message_id, text, actor)
        db.enqueue_message(m)
        row = db.connect().execute(
            "SELECT * FROM inbox_messages WHERE message_id=?", (message_id,)
        ).fetchone()
        await pipeline.process(dict(row))
        result = db.connect().execute(
            "SELECT processed_result FROM inbox_messages WHERE message_id=?", (message_id,)
        ).fetchone()["processed_result"]
        print(f"  [{result}] {actor[2]}：{text[:38]}")
        for r in replies:
            print(f"      {r}")
        print()

    def db_summary(title: str) -> None:
        tickets = db.connect().execute(
            "SELECT ticket_no, status, version FROM tickets ORDER BY id"
        ).fetchall()
        diagnoses = db.connect().execute(
            "SELECT ticket_id, items_json, is_current FROM diagnosis_versions WHERE is_current=1"
        ).fetchall()
        repairs = db.connect().execute(
            "SELECT ticket_id, repair_method, order_no FROM repair_method_versions WHERE is_current=1"
        ).fetchall()
        deliveries = db.connect().execute(
            "SELECT order_no, status FROM delivery_confirmations ORDER BY id"
        ).fetchall()
        print(f"  --- {title} ---")
        print(f"    工单: {[dict(t) for t in tickets]}")
        if diagnoses:
            print(f"    当前诊断: {[dict(d) for d in diagnoses]}")
        if repairs:
            print(f"    当前维修方式: {[dict(r) for r in repairs]}")
        if deliveries:
            print(f"    快递确认: {[dict(x) for x in deliveries]}")
        print()

    print("=" * 72)
    print("  钉钉报修工单系统 · 完整流程模拟（新流程 2026-08-12）")
    print("=" * 72)
    print()

    # ── 1. 店员报修（新流程：店铺同事可直接报修）──
    print("【1】店员报修（自然语言 → 建单）")
    await send("m1", "博物馆奇妙夜第一个房间消防门下沉明显，开门时剐蹭，3天内修",
               OT, preset=_decision("ticket.create", {
                   "subject": "博物馆奇妙夜", "location": "第一个房间消防门",
                   "problem_description": "下沉明显，开门剐蹭", "sla": "3天"}))
    db_summary("建单后")

    # ── 2. 第二张工单（同群多工单并行）──
    print("【2】店员再报修一张（同群第二张活动工单）")
    await send("m2", "二楼仓库的门锁也打不开了，位置在二楼仓库，麻烦3天内修",
               OT, preset=_decision("ticket.create", {
                   "subject": "仓库门锁", "location": "二楼仓库",
                   "problem_description": "门锁打不开", "sla": "3天"}))
    db_summary("多工单并存")

    # ── 2.5 查询当前活动工单 ──
    print("【2.5】店长查询当前活动工单（应列出两张）")
    await send("m2x", "#查询工单", MG)
    print()

    # ── 3. 工程师选单 + 故障判断 ──
    print("【3】工程师选中第一张工单，给出故障判断")
    await send("m3", "#选择工单 钉钉消息测试-博物馆奇妙夜-3天-001", EG)
    await send("m4", "门下沉了，我判断是合页松动", EG,
               preset=_decision("ticket.diagnosis.submit",
                                {"diagnosis_items": ["门体下沉", "上侧合页松动"]}))
    db_summary("故障判断后")

    # ── 4. 工程师维修方式 + 订单号 → 触发快递确认 ──
    print("【4】工程师提交维修方式+淘宝订单号 → 触发快递签收确认")
    await send("m5", "这个门打算淘宝采购后自行维修，订单号TB-2024-0001", EG,
               preset=_decision("ticket.repair_plan.submit", {
                   "repair_method": "淘宝采购后自行维修", "order_no": "TB-2024-0001"}))
    db_summary("维修方式 + 快递确认触发")

    # ── 5. 发起人（报修的店员）回复签收 ──
    print("【5】报修人回复快递已签收")
    await send("m6", "快递已经签收了", OT)
    db_summary("签收确认后")

    # ── 6. 店长切换到第二张工单，补充信息 ──
    print("【6】店长切换到第二张工单，补充钥匙也断了")
    await send("m7", "#选择工单 钉钉消息测试-仓库门锁-3天-002", MG)
    await send("m8", "门锁的钥匙也断了，补充一下", MG,
               preset=_decision("ticket.add_detail", {"content": "钥匙也断了"}))
    db_summary("补充后")

    # ── 7. 工程师完工（新策略：直接完成，无需确认）──
    print("【7】工程师回复博物馆的门修好了 → 直接完成（无需二次确认）")
    await send("m9", "博物馆的门修好了，测试正常", EG,
               preset=_decision("ticket.complete", {"completion_note": "已维修测试正常"},
                                ticket_no="钉钉消息测试-博物馆奇妙夜-3天-001"))
    db_summary("第一张完成")

    # ── 8. 店长取消第二张（高危，需确认）──
    print("【8】店长取消第二张工单（取消需确认）")
    await send("m10", "把仓库门锁那张取消了吧，是误报", MG,
               preset=_decision("ticket.cancel",
                                {"cancel_reason": "误报"},
                                ticket_no="钉钉消息测试-仓库门锁-3天-002"))
    print("      ↑ 已建待确认，等店长回复「确认」")
    print()
    await send("m11", "确认", MG, preset=_decision("system.confirm_pending_action"))
    db_summary("取消确认后")

    # ── 9. 查询工单 ──
    print("【9】查询当前工单")
    await send("m12", "#查询工单", MG)

    print("=" * 72)
    print("  模拟完成 · 数据库汇总")
    print("=" * 72)
    for t in db.connect().execute("SELECT * FROM tickets ORDER BY id").fetchall():
        t = dict(t)
        print(f"  {t['ticket_no']:32s} {t['status']:12s} v{t['version']}")
    db.close()


if __name__ == "__main__":
    asyncio.run(main())
