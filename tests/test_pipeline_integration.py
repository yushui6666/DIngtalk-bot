"""主链路集成测试：消息 → 建单 → 推进 → 完成 → 确认 → 死信。

覆盖 v4.0 核心主功能（Task 5-11），关键词快路径 + 注入的假分类器。
"""

from __future__ import annotations

import asyncio
from datetime import datetime
from pathlib import Path

import pytest

from db import Database
from models import NormalizedMessage
from notifier import Notifier
from pipeline import MessageProcessingPipeline, RuntimeMode
from routing.pending_actions import PendingActionService
from routing.ticket_contexts import TicketContextStore
from routing.ticket_router import TicketRouter
from semantics.protocol_loader import load_protocol
from semantics.types import SemanticDecision
from tickets.executor import TicketCommandExecutor
from tickets.repository import TicketRepository

_PROTOCOL_PATH = Path(__file__).resolve().parent.parent / "protocols" / "ticket_semantics.v4.json"

GROUP = {"group_id": "G1", "store_name": "钉钉消息测试",
         "manager_ids": ["uid-mgr"], "engineer_ids": ["uid-eng"], "other_member_ids": []}


class FakeClassifier:
    """按 message_id 返回预设语义决策；未预设则返回 chat.ignore。"""

    def __init__(self) -> None:
        self.responses: dict[str, SemanticDecision] = {}

    async def classify(self, message, candidates=None, pending_action=None, history=None) -> SemanticDecision:
        return self.responses.get(
            message.message_id,
            SemanticDecision(protocol_version="4.0.0", source="SEMANTIC_MODEL",
                             intent="chat.ignore", target_ticket_no=None, intent_confidence=0.0),
        )


@pytest.fixture()
def env(tmp_path):
    db = Database(tmp_path / "test.db")
    db.init_schema()
    db.upsert_group(GROUP)
    protocol = load_protocol(_PROTOCOL_PATH)
    repo = TicketRepository(db)
    router = TicketRouter()
    context = TicketContextStore(db)
    pending = PendingActionService(db)
    executor = TicketCommandExecutor(db, repo)
    sent: list[str] = []
    notifier = Notifier(db, lambda target, text: sent.append(text))
    classifier = FakeClassifier()

    def make_pipeline(mode=RuntimeMode.PRODUCTION, max_attempts=3):
        return MessageProcessingPipeline(
            db=db, repo=repo, protocol=protocol, router=router, context=context,
            pending=pending, executor=executor, notifier=notifier,
            classifier=classifier, mode=mode, max_attempts=max_attempts,
        )

    async def process(text, message_id, role="MANAGER", sender="uid-mgr"):
        msg = NormalizedMessage(
            message_id=message_id, group_id="G1", sender_id=sender, sender_name="u",
            content=text, message_type="text", sent_at=datetime.now(), sender_role=role,
        )
        db.enqueue_message(msg)
        row = db.connect().execute(
            "SELECT * FROM inbox_messages WHERE message_id=?", (message_id,)
        ).fetchone()
        return await make_pipeline().process(dict(row))

    yield SimpleNamespace(
        db=db, sent=sent, classifier=classifier, process=process,
        make_pipeline=make_pipeline, context=context,
    )
    db.close()


class SimpleNamespace:
    def __init__(self, **kw):
        self.__dict__.update(kw)


def _active(db: Database) -> list[dict]:
    return db.list_active_tickets("G1")


def _ticket(db: Database, ticket_no: str) -> dict:
    t = db.get_ticket_by_no(ticket_no)
    assert t is not None, f"工单不存在: {ticket_no}"
    return t


@pytest.mark.asyncio
async def test_create_ticket_via_keyword(env):
    status = await env.process("#报修\n主题：收银机\n位置：前台\n问题描述：死机\n时效：1天", "m1")
    assert status == "COMPLETED"
    t = _ticket(env.db, "钉钉消息测试-收银机-1天-001")
    assert t["status"] == "ACTIVE" and t["version"] == 1
    assert t["sla_days"] == 1
    assert t["current_deadline_at"] == t["initial_deadline_at"]  # 创建时截止=初始截止
    assert t["current_deadline_at"] > datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    assert any("已创建工单" in s for s in env.sent)


@pytest.mark.asyncio
async def test_create_defaults_sla_to_one_day(env):
    """报修未写时效时默认 1 天（业务决策 2026-08-12）。"""
    await env.process("#报修\n主题：收银机\n位置：前台\n问题描述：死机", "m1")
    act = _active(env.db)
    assert len(act) == 1
    assert act[0]["sla_days"] == 1
    # 截止 = 建单时间 + 1 天
    from datetime import datetime, timedelta

    expected = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
    assert act[0]["current_deadline_at"].startswith(expected)


@pytest.mark.asyncio
async def test_create_with_explicit_sla_respected(env):
    """用户明确写时效时按用户值。"""
    await env.process("#报修\n主题：收银机\n位置：前台\n问题描述：死机\n时效：7天", "m1")
    act = _active(env.db)
    assert act[0]["sla_days"] == 7


@pytest.mark.asyncio
async def test_engineer_create_rejected(env):
    await env.process("#报修\n主题：A\n位置：1\n问题描述：x\n时效：3天", "m1")
    status = await env.process("#报修\n主题：B\n位置：2\n问题描述：y\n时效：3天",
                               "m2", role="ENGINEER", sender="uid-eng")
    row = env.db.connect().execute(
        "SELECT processed_result FROM inbox_messages WHERE message_id='m2'").fetchone()
    assert row["processed_result"] == "REJECTED"
    assert len(_active(env.db)) == 1


@pytest.mark.asyncio
async def test_multiple_active_tickets_same_group(env):
    await env.process("#报修\n主题：收银机\n位置：前台\n问题描述：死机\n时效：1天", "m1")
    await env.process("#报修\n主题：门锁\n位置：二楼\n问题描述：打不开\n时效：3天", "m2")
    act = _active(env.db)
    assert len(act) == 2
    assert {t["ticket_no"] for t in act} == {"钉钉消息测试-收银机-1天-001", "钉钉消息测试-门锁-3天-002"}


@pytest.mark.asyncio
async def test_add_detail_and_complete(env):
    await env.process("#报修\n主题：收银机\n位置：前台\n问题描述：死机\n时效：1天", "m1")
    t1 = _active(env.db)[0]
    await env.process("#补充 工单编号：钉钉消息测试-收银机-1天-001 内容：屏幕闪烁", "m2")
    t = _ticket(env.db, t1["ticket_no"])
    assert "屏幕闪烁" in t["problem_description"] and t["version"] == 2
    await env.process("#完毕 工单编号：钉钉消息测试-收银机-1天-001", "m3")
    assert _ticket(env.db, t1["ticket_no"])["status"] == "COMPLETED"


@pytest.mark.asyncio
async def test_select_context_then_route_by_context(env):
    await env.process("#报修\n主题：收银机\n位置：前台\n问题描述：死机\n时效：1天", "m1")
    await env.process("#报修\n主题：门锁\n位置：二楼\n问题描述：打不开\n时效：3天", "m2")
    await env.process("#选择工单 钉钉消息测试-门锁-3天-002", "m3", role="ENGINEER", sender="uid-eng")
    # 双候选 + 无编号的补充 → 走用户上下文归属门锁
    await env.process("#补充 内容：换了新锁芯", "m4", role="ENGINEER", sender="uid-eng")
    t = _ticket(env.db, "钉钉消息测试-门锁-3天-002")
    assert "新锁芯" in t["problem_description"]
    other = _ticket(env.db, "钉钉消息测试-收银机-1天-001")
    assert "新锁芯" not in other["problem_description"]


@pytest.mark.asyncio
async def test_multi_candidate_without_number_clarifies(env):
    await env.process("#报修\n主题：收银机\n位置：前台\n问题描述：死机\n时效：1天", "m1")
    await env.process("#报修\n主题：门锁\n位置：二楼\n问题描述：打不开\n时效：3天", "m2")
    status = await env.process("#补充 内容：又出问题了", "m3")
    row = env.db.connect().execute(
        "SELECT processed_result FROM inbox_messages WHERE message_id='m3'").fetchone()
    assert row["processed_result"] == "CLARIFY"
    assert any("多张工单" in s for s in env.sent)


@pytest.mark.asyncio
async def test_natural_language_without_model_ignored(env):
    status = await env.process("麻烦帮我查一下现在的工单情况", "m1")
    row = env.db.connect().execute(
        "SELECT processed_result FROM inbox_messages WHERE message_id='m1'").fetchone()
    assert row["processed_result"] == "IGNORED"


@pytest.mark.asyncio
async def test_model_complete_executes_directly(env):
    """业务决策（2026-08-12）：自然语言说「修好了」即直接完成，不再强制二次确认。"""
    await env.process("#报修\n主题：收银机\n位置：前台\n问题描述：死机\n时效：1天", "m1")
    t1 = _active(env.db)[0]
    # 模型说“完成”→ 协议已改 NOT_REQUIRED → 直接执行
    env.classifier.responses["m2"] = SemanticDecision(
        protocol_version="4.0.0", source="SEMANTIC_MODEL", intent="ticket.complete",
        target_ticket_no=t1["ticket_no"], intent_confidence=0.95)
    await env.process("维修已经完成了", "m2")
    row = env.db.connect().execute(
        "SELECT processed_result FROM inbox_messages WHERE message_id='m2'").fetchone()
    assert row["processed_result"] == "EXECUTED"
    assert _ticket(env.db, t1["ticket_no"])["status"] == "COMPLETED"
    # 未创建待确认
    assert env.db.get_waiting_pending("G1", "uid-mgr") is None
    assert any("已完成" in s for s in env.sent)


@pytest.mark.asyncio
async def test_suffix_number_routes_to_correct_ticket(env):
    """同群多张同主题工单时，消息里的短编号（如「002」）归到对应那张（2026-08-13）。"""
    await env.process("#报修\n主题：博物馆奇妙夜\n位置：一楼\n问题描述：门坏了\n时效：1天", "m1")
    await env.process("#报修\n主题：博物馆奇妙夜\n位置：二楼\n问题描述：门又坏了\n时效：3天", "m2")
    act = _active(env.db)
    assert len(act) == 2
    t002 = next(t for t in act if t["ticket_no"].endswith("-002"))
    # 模型识别为完成但没带编号；消息里的「002」应路由到 3天-002
    env.classifier.responses["m3"] = SemanticDecision(
        protocol_version="4.0.0", source="SEMANTIC_MODEL", intent="ticket.complete",
        target_ticket_no=None, intent_confidence=0.95)
    await env.process("002完成了", "m3")
    assert _ticket(env.db, t002["ticket_no"])["status"] == "COMPLETED"
    other = next(t for t in act if t["id"] != t002["id"])
    assert _ticket(env.db, other["ticket_no"])["status"] == "ACTIVE"


@pytest.mark.asyncio
async def test_number_reply_selects_nth_candidate_locally(env):
    """AI 列候选后回「2」→ 本地直接选第 2 个候选，不调模型（2026-08-13）。"""
    await env.process("#报修\n主题：博物馆奇妙夜\n位置：一楼\n问题描述：门坏了\n时效：1天", "m1")
    await env.process("#报修\n主题：博物馆奇妙夜\n位置：二楼\n问题描述：门又坏了\n时效：3天", "m2")
    act = _active(env.db)
    assert len(act) == 2
    await env.process("2", "m3")
    ctx = env.context.get_active("G1", "uid-mgr", datetime.now())
    assert ctx == act[1]["id"]  # 第 2 个候选
    assert any(f"已切换到工单 {act[1]['ticket_no']}" in s for s in env.sent)
    # 中文「第二个」同样生效
    await env.process("第一个", "m4")
    ctx2 = env.context.get_active("G1", "uid-mgr", datetime.now())
    assert ctx2 == act[0]["id"]


@pytest.mark.asyncio
async def test_model_cancel_still_requires_confirmation(env):
    """取消/重开仍保持确认策略（高危误操作防护）。"""
    await env.process("#报修\n主题：收银机\n位置：前台\n问题描述：死机\n时效：1天", "m1")
    t1 = _active(env.db)[0]
    env.classifier.responses["m2"] = SemanticDecision(
        protocol_version="4.0.0", source="SEMANTIC_MODEL", intent="ticket.cancel",
        target_ticket_no=t1["ticket_no"], intent_confidence=0.95,
        fields={"cancel_reason": "误报"})
    await env.process("帮我把这个误报工单取消掉", "m2")
    row = env.db.connect().execute(
        "SELECT processed_result FROM inbox_messages WHERE message_id='m2'").fetchone()
    assert row["processed_result"] == "WAITING_CONFIRMATION"
    assert _ticket(env.db, t1["ticket_no"])["status"] == "ACTIVE"
    # 确认提示用中文可读文案，不暴露 intent ID
    assert any("确认执行「取消工单」" in s for s in env.sent)
    assert not any("ticket.cancel" in s for s in env.sent)


@pytest.mark.asyncio
async def test_system_clarify_shows_readable_message(env):
    """system.clarify（消息有歧义）→ 直接可读澄清提示，不建「确认执行」待办。"""
    env.classifier.responses["m1"] = SemanticDecision(
        protocol_version="4.0.0", source="SEMANTIC_MODEL", intent="system.clarify",
        target_ticket_no=None, intent_confidence=0.9,
        evidence=("报修", "完毕"))
    await env.process("先报修一下门坏了，然后又完毕了", "m1")
    row = env.db.connect().execute(
        "SELECT processed_result FROM inbox_messages WHERE message_id='m1'").fetchone()
    assert row["processed_result"] == "CLARIFY"
    # 不创建待确认，也不出现「确认执行：system.clarify」
    assert env.db.get_waiting_pending("G1", "uid-mgr") is None
    assert any("有歧义" in s for s in env.sent)
    assert not any("system.clarify" in s for s in env.sent)


@pytest.mark.asyncio
async def test_model_failure_retries_then_dead_letter(env):
    # 未配置模型的聊天消息 → 分类器返回降级 fallback → 重试 → 死信
    def failing_classify(message, candidates=None, pending_action=None):
        raise RuntimeError("network down")

    # 用失败分类器替换
    env.classifier.classify = failing_classify
    pl = env.make_pipeline(max_attempts=3)
    msg = NormalizedMessage(message_id="m1", group_id="G1", sender_id="uid-mgr",
                            sender_name="u", content="随便聊聊", message_type="text",
                            sent_at=datetime.now(), sender_role="MANAGER")
    env.db.enqueue_message(msg)
    for i in range(3):
        row = env.db.connect().execute(
            "SELECT * FROM inbox_messages WHERE message_id='m1'").fetchone()
        status = await pl.process(dict(row))
        final_row = env.db.connect().execute(
            "SELECT status, attempts FROM inbox_messages WHERE message_id='m1'").fetchone()
        print(f"  重试 {i + 1}: status={final_row['status']} attempts={final_row['attempts']}")
        if final_row["status"] == "DEAD_LETTER":
            break
    final = env.db.connect().execute(
        "SELECT status, attempts, processed_result FROM inbox_messages WHERE message_id='m1'").fetchone()
    assert final["status"] == "DEAD_LETTER"
    assert final["attempts"] == 3


@pytest.mark.asyncio
async def test_shadow_mode_records_but_does_not_execute(env):
    pl = env.make_pipeline(mode=RuntimeMode.SHADOW)
    msg = NormalizedMessage(message_id="m1", group_id="G1", sender_id="uid-mgr",
                            sender_name="u", content="#报修\n主题：收银机\n位置：前台\n问题描述：死机\n时效：1天",
                            message_type="text", sent_at=datetime.now(), sender_role="MANAGER")
    env.db.enqueue_message(msg)
    row = env.db.connect().execute("SELECT * FROM inbox_messages WHERE message_id='m1'").fetchone()
    await pl.process(dict(row))
    assert len(_active(env.db)) == 0
    dec = env.db.connect().execute(
        "SELECT intent FROM semantic_decisions WHERE message_id='m1'").fetchone()
    assert dec is not None and dec["intent"] == "ticket.create"
