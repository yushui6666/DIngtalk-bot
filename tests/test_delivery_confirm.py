"""快递签收确认集成测试（业务：淘宝采购后向发起人确认快递是否签收）。"""

from __future__ import annotations

import asyncio
from datetime import datetime
from pathlib import Path

import pytest

from db import Database
from models import NormalizedMessage
from notifier import Notifier
from pipeline import MessageProcessingPipeline, RuntimeMode
from routing.delivery import DeliveryConfirmService
from routing.pending_actions import PendingActionService
from routing.ticket_contexts import TicketContextStore
from routing.ticket_router import TicketRouter
from semantics.protocol_loader import load_protocol
from tickets.executor import TicketCommandExecutor
from tickets.repository import TicketRepository

_PROTOCOL_PATH = Path(__file__).resolve().parent.parent / "protocols" / "ticket_semantics.v4.json"
GROUP = {"group_id": "G1", "store_name": "钉钉消息测试",
         "manager_ids": ["uid-mgr"], "engineer_ids": ["uid-eng"], "other_member_ids": []}


@pytest.fixture()
def env(tmp_path):
    db = Database(tmp_path / "delivery.db")
    db.init_schema()
    db.upsert_group(GROUP)
    protocol = load_protocol(_PROTOCOL_PATH)
    repo = TicketRepository(db)
    router = TicketRouter()
    context = TicketContextStore(db)
    pending = PendingActionService(db)
    executor = TicketCommandExecutor(db, repo)
    delivery = DeliveryConfirmService(db)
    sent: list[str] = []
    notifier = Notifier(db, lambda target, text: sent.append(text))
    pipeline = MessageProcessingPipeline(
        db=db, repo=repo, protocol=protocol, router=router, context=context,
        pending=pending, executor=executor, notifier=notifier,
        classifier=None, delivery=delivery, mode=RuntimeMode.PRODUCTION,
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
        return await pipeline.process(dict(row))

    yield SimpleNamespace(db=db, sent=sent, process=process, delivery=delivery, pipeline=pipeline)
    db.close()


class SimpleNamespace:
    def __init__(self, **kw):
        self.__dict__.update(kw)


async def _create_ticket(env) -> dict:
    await env.process("#报修\n主题：收银机\n位置：前台\n问题描述：死机\n时效：1天", "create")
    return env.db.list_active_tickets("G1")[0]


async def _submit_repair_with_order(env):
    await env.process(
        "#维修方式\n维修方式：淘宝采购后自行维修\n订单号：TB-2024-0001",
        "repair", role="ENGINEER", sender="uid-eng",
    )


@pytest.mark.asyncio
async def test_repair_order_triggers_delivery_confirmation(env):
    ticket = await _create_ticket(env)
    await _submit_repair_with_order(env)
    rows = env.db.connect().execute(
        "SELECT ticket_id, order_no, confirm_user_id, status FROM delivery_confirmations"
    ).fetchall()
    assert len(rows) == 1
    row = dict(rows[0])
    assert row["ticket_id"] == ticket["id"]
    assert row["order_no"] == "TB-2024-0001"
    assert row["confirm_user_id"] == ticket["reporter_id"]  # 询问发起人
    assert row["status"] == "WAITING"
    assert any("快递签收确认" in s for s in env.sent)


@pytest.mark.asyncio
async def test_same_order_not_asked_twice(env):
    await _create_ticket(env)
    await _submit_repair_with_order(env)
    await _submit_repair_with_order(env)  # 重复提交同单号
    count = env.db.connect().execute(
        "SELECT COUNT(*) FROM delivery_confirmations WHERE status='WAITING'"
    ).fetchone()[0]
    assert count == 1


@pytest.mark.asyncio
async def test_reporter_reply_signed_resolves(env):
    await _create_ticket(env)
    await _submit_repair_with_order(env)
    await env.process("快递已签收了", "reply")  # 发起人回复
    row = env.db.connect().execute(
        "SELECT status FROM delivery_confirmations WHERE order_no='TB-2024-0001'"
    ).fetchone()
    assert row["status"] == "SIGNED"
    assert any("已签收" in s for s in env.sent)


@pytest.mark.asyncio
async def test_reporter_reply_unsigned_resolves(env):
    await _create_ticket(env)
    await _submit_repair_with_order(env)
    await env.process("还没收到，可能路上", "reply")
    row = env.db.connect().execute(
        "SELECT status FROM delivery_confirmations WHERE order_no='TB-2024-0001'"
    ).fetchone()
    assert row["status"] == "UNSIGNED"


@pytest.mark.asyncio
async def test_unrelated_reply_does_not_resolve(env):
    ticket = await _create_ticket(env)
    await _submit_repair_with_order(env)
    await env.process("好的知道了，辛苦", "reply")
    row = env.db.connect().execute(
        "SELECT status FROM delivery_confirmations WHERE order_no='TB-2024-0001'"
    ).fetchone()
    assert row["status"] == "WAITING"  # 未命中关键词，不解析
    # 且不影响该工单
    assert env.db.get_ticket(ticket["id"])["status"] == "ACTIVE"


@pytest.mark.asyncio
async def test_complete_expires_waiting_delivery(env):
    await _create_ticket(env)
    await _submit_repair_with_order(env)
    await env.process("#完毕 工单编号：钉钉消息测试-收银机-1天-001", "done")
    row = env.db.connect().execute(
        "SELECT status FROM delivery_confirmations WHERE order_no='TB-2024-0001'"
    ).fetchone()
    assert row["status"] == "EXPIRED"


@pytest.mark.asyncio
async def test_delivery_reply_enriched_with_taobao_order(env):
    """对账表有该订单 → 快递确认带出订单状态/物流/收货地址。"""
    env.db.upsert_taobao_order(
        order_id="TB-2024-0001",
        product_summary="合页×2",
        tracking_number="79025196042648",
        address="上海 黄浦区 人民大道221号",
        status="卖家已发货",
        source="测试",
    )
    await _create_ticket(env)
    await _submit_repair_with_order(env)
    assert any("订单状态：卖家已发货" in s for s in env.sent)
    assert any("物流单号 79025196042648" in s for s in env.sent)
    assert any("收货地址：上海 黄浦区" in s for s in env.sent)


@pytest.mark.asyncio
async def test_delivery_reply_warns_unknown_order(env):
    """对账表没有该订单 → 提醒核对订单号。"""
    await _create_ticket(env)
    await env.process(
        "#维修方式\n维修方式：淘宝采购后自行维修\n订单号：XYZ-NOT-FOUND-1",
        "repair", role="ENGINEER", sender="uid-eng",
    )
    assert any("未在淘宝对账数据中找到该订单号" in s for s in env.sent)
