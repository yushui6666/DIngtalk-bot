"""advisor 单测与 pipeline 集成：建单→建议→台账→顺序（静态向量，不调 API）。"""

from __future__ import annotations

from datetime import datetime

import numpy as np
import pytest

from db import Database
from qa.advisor import TicketAdvisor
from qa.kb_store import KBStore


def _vec(seed: int, dim: int = 8) -> np.ndarray:
    rng = np.random.default_rng(seed)
    v = rng.standard_normal(dim).astype(np.float32)
    return v / np.linalg.norm(v)


@pytest.fixture()
def store(tmp_path):
    s = KBStore(tmp_path / "kb.db")
    s.init_schema()
    yield s
    s.close()


def _seed_case(store, doc_id, *, ticket_no, diagnosis, repair, seed):
    store.upsert_document(
        doc_id=doc_id, source_type="TICKET_CASE", title=f"案例{ticket_no}",
        content=f"冷柜不制冷 {diagnosis} {repair}",
        metadata={"ticket_no": ticket_no, "diagnosis": diagnosis,
                  "repair_method": repair},
    )
    store.save_embedding(doc_id, _vec(seed))


@pytest.fixture()
def db(tmp_path):
    d = Database(tmp_path / "t.db")
    d.init_schema()
    d.upsert_group({"group_id": "G1", "store_name": "测试店",
                    "manager_ids": ["mgr"], "engineer_ids": ["eng"]})
    with d.transaction("t") as conn:
        tid = d.insert_ticket({
            "ticket_no": "W100", "group_id": "G1", "store_name": "测试店",
            "reporter_id": "mgr", "subject": "冷柜", "location": "后厨",
            "problem_description": "不制冷，压缩机嗡嗡响", "sla_days": 3,
            "initial_deadline_at": "2026-08-01 00:00:00",
            "current_deadline_at": "2026-08-04 00:00:00",
            "status": "ACTIVE",
        })
        conn.execute("UPDATE tickets SET id=id WHERE id=?", (tid,))
    yield d
    d.close()


def _ticket(db, ticket_no="W100") -> dict:
    return db.get_ticket_by_no(ticket_no)


class TestAdvisorUnit:
    def test_advice_with_similar_case(self, db, store):
        _seed_case(store, "ticket:W001", ticket_no="W001",
                   diagnosis="制冷剂泄漏", repair="补充制冷剂+清洗冷凝器", seed=1)
        advisor = TicketAdvisor(db, store)
        advice = advisor.advise_for_new_ticket(_ticket(db))
        assert advice is not None
        assert "W001" in advice["text"]
        assert "制冷剂泄漏" in advice["text"]
        assert "补充制冷剂" in advice["text"]
        assert "未解决" in advice["text"]  # 反馈指引
        # 台账已记录
        row = db.get_latest_suggestion(_ticket(db)["id"])
        assert row is not None and row["feedback"] is None
        assert "ticket:W001" in row["doc_ids"]

    def test_silent_when_no_similar(self, db, store):
        # 语料与查询（冷柜/后厨/不制冷/压缩机嗡嗡响）零重叠 → FTS 无命中，
        # 且无向量嵌入 → best_cos=0 低于阈值 → 整体拒答沉默
        store.upsert_document(
            doc_id="ticket:W777", source_type="TICKET_CASE",
            title="空调内机漏水", content="空调内机漏水排水管堵塞疏通解决",
            metadata={"ticket_no": "W777", "diagnosis": "排水管堵塞",
                      "repair_method": "疏通排水管"},
        )
        store.save_embedding("ticket:W777", _vec(50))
        advisor = TicketAdvisor(db, store)
        assert advisor.advise_for_new_ticket(_ticket(db)) is None
        assert db.get_latest_suggestion(_ticket(db)["id"]) is None

    def test_disabled_returns_none(self, db, store):
        advisor = TicketAdvisor(db, store, enabled=False)
        assert advisor.advise_for_new_ticket(_ticket(db)) is None

    def test_exception_silent_degrade(self, db):
        class BrokenStore:
            def connect(self):
                raise RuntimeError("broken")
        advisor = TicketAdvisor(db, BrokenStore())
        assert advisor.advise_for_new_ticket(_ticket(db)) is None  # 不抛出


class TestPipelineIntegration:
    @pytest.fixture()
    def env(self, tmp_path, monkeypatch):
        import config as _config
        monkeypatch.setattr(_config, "ORDER_STORE_TABLE_PATH", tmp_path / "x.xlsx")
        monkeypatch.setattr(_config, "TAOBAO_ORDER_DETAIL_XLSX", tmp_path / "y.xlsx")
        monkeypatch.setattr(_config, "TAOBAO_PENDING_XLSX", tmp_path / "z.xlsx")

        db = Database(tmp_path / "t.db")
        db.init_schema()
        db.upsert_group({"group_id": "G1", "store_name": "测试店",
                         "manager_ids": ["mgr"], "engineer_ids": ["eng"]})
        from notifier import Notifier
        sent: list[str] = []
        notifier = Notifier(db, lambda target, text: sent.append(text))

        store = KBStore(tmp_path / "kb.db")
        store.init_schema()
        advisor = TicketAdvisor(db, store)
        yield _Env(db, sent, notifier, store, advisor)
        db.close()
        store.close()

    def _make_pipeline(self, env):
        from pipeline import MessageProcessingPipeline, RuntimeMode
        from routing.pending_actions import PendingActionService
        from routing.ticket_contexts import TicketContextStore
        from routing.ticket_router import TicketRouter
        from semantics.protocol_loader import load_protocol
        from tickets.executor import TicketCommandExecutor
        from tickets.repository import TicketRepository
        from pathlib import Path
        protocol = load_protocol(Path("protocols/ticket_semantics.v4.json"))
        return MessageProcessingPipeline(
            db=env.db, repo=TicketRepository(env.db), protocol=protocol,
            router=TicketRouter(), context=TicketContextStore(env.db),
            pending=PendingActionService(env.db),
            executor=TicketCommandExecutor(env.db, TicketRepository(env.db)),
            notifier=env.notifier, advisor=env.advisor,
            mode=RuntimeMode.PRODUCTION,
        )

    @pytest.mark.asyncio
    async def test_create_then_advice_order(self, env):
        import asyncio
        _seed_case(env.store, "ticket:W001", ticket_no="W001",
                   diagnosis="制冷剂泄漏", repair="补充制冷剂", seed=1)
        pl = self._make_pipeline(env)
        from models import NormalizedMessage
        msg = NormalizedMessage(
            message_id="m1", group_id="G1", sender_id="mgr", sender_name="u",
            content="#报修\n主题：冷柜\n位置：后厨\n问题描述：不制冷，压缩机嗡嗡响\n时效：3天",
            message_type="text", sent_at=datetime.now(), sender_role="MANAGER",
        )
        env.db.enqueue_message(msg)
        row = env.db.connect().execute(
            "SELECT * FROM inbox_messages WHERE message_id='m1'").fetchone()
        await pl.process(dict(row))
        assert len(env.sent) == 2                       # 建单回执 + 建议
        assert "已创建工单" in env.sent[0]
        assert "相似案例参考" in env.sent[1]
        # 台账
        ticket = env.db.connect().execute(
            "SELECT * FROM tickets WHERE group_id='G1'").fetchone()
        assert env.db.get_latest_suggestion(ticket["id"]) is not None

    @pytest.mark.asyncio
    async def test_create_without_kb_still_creates(self, env):
        """空知识库：无建议，建单照常（沉默降级）。"""
        import asyncio
        pl = self._make_pipeline(env)
        from models import NormalizedMessage
        msg = NormalizedMessage(
            message_id="m2", group_id="G1", sender_id="mgr", sender_name="u",
            content="#报修\n主题：空调\n位置：大厅\n问题描述：漏水\n时效：1天",
            message_type="text", sent_at=datetime.now(), sender_role="MANAGER",
        )
        env.db.enqueue_message(msg)
        row = env.db.connect().execute(
            "SELECT * FROM inbox_messages WHERE message_id='m2'").fetchone()
        status = await pl.process(dict(row))
        assert status == "COMPLETED"
        assert len(env.sent) == 1 and "已创建工单" in env.sent[0]


class _Env:
    def __init__(self, db, sent, notifier, store, advisor):
        self.db, self.sent, self.notifier = db, sent, notifier
        self.store, self.advisor = store, advisor
