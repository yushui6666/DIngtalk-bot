"""反馈与升级（v4.3 任务 6）：#未解决 升级、「解决了」AI 建议自动落档。"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import numpy as np
import pytest

from db import Database
from notifier import Notifier
from pipeline import MessageProcessingPipeline, RuntimeMode
from routing.pending_actions import PendingActionService
from routing.ticket_contexts import TicketContextStore
from routing.ticket_router import TicketRouter
from semantics.keyword_matcher import match_keyword
from semantics.protocol_loader import load_protocol
from tickets.executor import TicketCommandExecutor
from tickets.repository import TicketRepository

from qa.advisor import TicketAdvisor
from qa.kb_store import KBStore
from test_pipeline_integration import FakeClassifier

GROUP = {"group_id": "G1", "store_name": "测试店",
         "manager_ids": ["mgr"], "engineer_ids": ["eng"]}

CREATE_MSG = """#报修
主题：冷柜
位置：后厨
问题描述：不制冷，压缩机嗡嗡响
时效：3天"""


def _vec(seed: int, dim: int = 8) -> np.ndarray:
    rng = np.random.default_rng(seed)
    v = rng.standard_normal(dim).astype(np.float32)
    return v / np.linalg.norm(v)


class TestKeywordFastPath:
    def test_unresolved_keyword_matches(self):
        protocol = load_protocol(Path("protocols/ticket_semantics.v4.json"))
        decision = match_keyword("#未解决 工单编号：W001", protocol)
        assert decision is not None
        assert decision.intent == "qa.unresolved"
        assert decision.source == "keyword"  # 快路径来源标记（小写）
        assert decision.target_ticket_no == "W001"

    def test_unresolved_keyword_prefix_not_matched(self):
        protocol = load_protocol(Path("protocols/ticket_semantics.v4.json"))
        assert match_keyword("#未解决了吗 W001", protocol) is None


class TestEscalationAndAutoRecord:
    @pytest.fixture()
    def env(self, tmp_path, monkeypatch):
        import config as _config
        monkeypatch.setattr(_config, "ORDER_STORE_TABLE_PATH", tmp_path / "x.xlsx")
        monkeypatch.setattr(_config, "TAOBAO_ORDER_DETAIL_XLSX", tmp_path / "y.xlsx")
        monkeypatch.setattr(_config, "TAOBAO_PENDING_XLSX", tmp_path / "z.xlsx")

        db = Database(tmp_path / "t.db")
        db.init_schema()
        db.upsert_group(GROUP)
        sent: list[str] = []
        notifier = Notifier(db, lambda target, text: sent.append(text))
        store = KBStore(tmp_path / "kb.db")
        store.init_schema()
        advisor = TicketAdvisor(db, store)

        protocol = load_protocol(Path("protocols/ticket_semantics.v4.json"))
        pipeline = MessageProcessingPipeline(
            db=db, repo=TicketRepository(db), protocol=protocol,
            router=TicketRouter(), context=TicketContextStore(db),
            pending=PendingActionService(db),
            executor=TicketCommandExecutor(db, TicketRepository(db)),
            notifier=notifier, advisor=advisor,
            classifier=FakeClassifier(protocol=protocol),
            mode=RuntimeMode.PRODUCTION,
        )

        async def process(text, message_id, role="MANAGER", sender="mgr"):
            from models import NormalizedMessage
            msg = NormalizedMessage(
                message_id=message_id, group_id="G1", sender_id=sender,
                sender_name="u", content=text, message_type="text",
                sent_at=datetime.now(), sender_role=role,
            )
            db.enqueue_message(msg)
            row = db.connect().execute(
                "SELECT * FROM inbox_messages WHERE message_id=?",
                (message_id,)).fetchone()
            return await pipeline.process(dict(row))

        yield _E(db, sent, store, advisor, process)
        db.close()
        store.close()

    def _seed_case(self, env):
        env.store.upsert_document(
            doc_id="ticket:W001", source_type="TICKET_CASE",
            title="冷柜不制冷", content="冷柜不制冷 制冷剂泄漏 补充制冷剂",
            metadata={"ticket_no": "W001", "diagnosis": "制冷剂泄漏",
                      "repair_method": "补充制冷剂"},
        )
        env.store.save_embedding("ticket:W001", _vec(1))

    def _create(self, env, mid="m1") -> dict:
        import asyncio
        asyncio.get_event_loop()
        return env.process(CREATE_MSG, mid)

    @pytest.mark.asyncio
    async def test_unresolved_escalates(self, env):
        self._seed_case(env)
        await env.process(CREATE_MSG, "m1")
        ticket = env.db.connect().execute(
            "SELECT * FROM tickets WHERE group_id='G1'").fetchone()
        assert ticket is not None

        status = await env.process(
            f"#未解决 工单编号：{ticket['ticket_no']}", "m2")
        assert status == "COMPLETED"
        # 升级标记
        updated = env.db.get_ticket(ticket["id"])
        assert updated["ai_escalated"] == 1
        sugg = env.db.get_latest_suggestion(ticket["id"])
        assert sugg["feedback"] == "UNRESOLVED"
        assert sugg["escalated_at"] is not None
        # 升级摘要（含故障与建议上下文）
        esc = [s for s in env.sent if "🚨" in s]
        assert len(esc) == 1
        assert ticket["ticket_no"] in esc[0]
        assert "冷柜" in esc[0]
        assert "制冷剂" in esc[0]  # 已给建议摘要

    @pytest.mark.asyncio
    async def test_resolved_autorecords_ai_suggestion(self, env):
        """决策6：「解决了」完单 → AI 建议自动落档为诊断/维修方式（engineer_id=AI）。"""
        self._seed_case(env)
        await env.process(CREATE_MSG, "m1")
        ticket = env.db.connect().execute(
            "SELECT * FROM tickets WHERE group_id='G1'").fetchone()

        status = await env.process(
            f"#完毕 工单编号：{ticket['ticket_no']}", "m2", role="MANAGER")
        assert status == "COMPLETED"
        # 建议标记 RESOLVED
        sugg = env.db.get_latest_suggestion(ticket["id"])
        assert sugg["feedback"] == "RESOLVED"
        # AI 落档
        diag = env.db.connect().execute(
            "SELECT * FROM diagnosis_versions WHERE ticket_id=? AND is_current=1",
            (ticket["id"],)).fetchone()
        assert diag is not None and diag["engineer_id"] == "AI"
        assert "制冷剂泄漏" in diag["items_json"]
        rep = env.db.connect().execute(
            "SELECT * FROM repair_method_versions WHERE ticket_id=? AND is_current=1",
            (ticket["id"],)).fetchone()
        assert rep is not None and rep["engineer_id"] == "AI"
        assert rep["repair_method"] == "补充制冷剂"

    @pytest.mark.asyncio
    async def test_engineer_diagnosis_not_overwritten(self, env):
        """工程师已给诊断时，完单不覆盖（人 > AI）。"""
        self._seed_case(env)
        await env.process(CREATE_MSG, "m1")
        ticket = env.db.connect().execute(
            "SELECT * FROM tickets WHERE group_id='G1'").fetchone()
        await env.process(
            f"#故障判断 工单编号：{ticket['ticket_no']} 故障判断：压缩机损坏",
            "m2", role="ENGINEER", sender="eng")
        await env.process(
            f"#完毕 工单编号：{ticket['ticket_no']}", "m3")
        diag = env.db.connect().execute(
            "SELECT * FROM diagnosis_versions WHERE ticket_id=? AND is_current=1",
            (ticket["id"],)).fetchone()
        assert diag["engineer_id"] == "eng"
        assert "压缩机损坏" in diag["items_json"]

    @pytest.mark.asyncio
    async def test_unresolved_without_suggestion_polite(self, env):
        """无 AI 建议的工单回「未解决」：礼貌说明，不升级。"""
        await env.process(CREATE_MSG, "m1")  # 空知识库 → 无建议
        ticket = env.db.connect().execute(
            "SELECT * FROM tickets WHERE group_id='G1'").fetchone()
        status = await env.process(
            f"#未解决 工单编号：{ticket['ticket_no']}", "m2")
        assert status == "COMPLETED"
        assert any("没有 AI 建议记录" in s for s in env.sent)
        assert env.db.get_ticket(ticket["id"])["ai_escalated"] == 1  # 仍标记升级


class _E:
    def __init__(self, db, sent, store, advisor, process):
        self.db, self.sent, self.store = db, sent, store
        self.advisor, self.process = advisor, process
