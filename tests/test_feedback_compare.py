"""隐式比对（任务 7）：建议原因 vs 工程师诊断，命中/偏差落库。"""

from __future__ import annotations

import pytest

from qa.feedback import compare_suggestion_with_diagnosis


def _sugg(causes):
    return {"detail": {"causes": causes}, "feedback": None}


class TestCompare:
    def test_exact_overlap_hits(self):
        r = compare_suggestion_with_diagnosis(_sugg(["制冷剂泄漏"]), ["制冷剂泄漏，需补充"])
        assert r["hit"] is True
        assert len(r["matched_items"]) == 1

    def test_partial_segment_hits(self):
        # 共享"冷凝器"三字片段
        r = compare_suggestion_with_diagnosis(_sugg(["冷凝器积灰"]), ["冷凝器脏堵导致不制冷"])
        assert r["hit"] is True

    def test_no_overlap_misses(self):
        r = compare_suggestion_with_diagnosis(_sugg(["制冷剂泄漏"]), ["门体下沉，合页松动"])
        assert r["hit"] is False
        assert r["matched_items"] == []

    def test_no_causes_never_hits(self):
        r = compare_suggestion_with_diagnosis(_sugg([]), ["制冷剂泄漏"])
        assert r["hit"] is False

    def test_punct_and_case_normalized(self):
        r = compare_suggestion_with_diagnosis(_sugg(["Compressor broken"]), ["compressor、broken！"])
        assert r["hit"] is True


class TestIntegration:
    @pytest.mark.asyncio
    async def test_diagnosis_triggers_compare(self, tmp_path, monkeypatch):
        """#故障判断 提交 → 建议的 detail.implicit_match 被写入。"""
        import config as _config
        monkeypatch.setattr(_config, "ORDER_STORE_TABLE_PATH", tmp_path / "x.xlsx")
        monkeypatch.setattr(_config, "TAOBAO_ORDER_DETAIL_XLSX", tmp_path / "y.xlsx")
        monkeypatch.setattr(_config, "TAOBAO_PENDING_XLSX", tmp_path / "z.xlsx")
        from datetime import datetime
        from db import Database
        from notifier import Notifier
        from pipeline import MessageProcessingPipeline, RuntimeMode
        from routing.pending_actions import PendingActionService
        from routing.ticket_contexts import TicketContextStore
        from routing.ticket_router import TicketRouter
        from semantics.protocol_loader import load_protocol
        from tickets.executor import TicketCommandExecutor
        from tickets.repository import TicketRepository

        db = Database(tmp_path / "t.db")
        db.init_schema()
        db.upsert_group({"group_id": "G1", "store_name": "测试店",
                         "manager_ids": ["mgr"], "engineer_ids": ["eng"]})
        with db.transaction("t"):
            tid = db.insert_ticket({
                "ticket_no": "W001", "group_id": "G1", "store_name": "测试店",
                "reporter_id": "mgr", "subject": "冷柜", "location": "后厨",
                "problem_description": "不制冷", "sla_days": 3,
                "initial_deadline_at": "2026-08-01 00:00:00",
                "current_deadline_at": "2026-08-04 00:00:00",
                "status": "ACTIVE",
            })
        db.record_suggestion(tid, ["ticket:W009"], 0.8, "建议文本",
                             detail={"causes": ["制冷剂泄漏"], "repairs": ["补充制冷剂"]})

        sent = []
        pipeline = MessageProcessingPipeline(
            db=db, repo=TicketRepository(db),
            protocol=load_protocol(Path("protocols/ticket_semantics.v4.json")
                                   if (Path := __import__("pathlib").Path) else None),
            router=TicketRouter(), context=TicketContextStore(db),
            pending=PendingActionService(db),
            executor=TicketCommandExecutor(db, TicketRepository(db)),
            notifier=Notifier(db, lambda t, x: sent.append(x)),
            mode=RuntimeMode.PRODUCTION,
        )
        from models import NormalizedMessage
        msg = NormalizedMessage(
            message_id="m1", group_id="G1", sender_id="eng", sender_name="e",
            content="#故障判断 工单编号：W001 故障判断：制冷剂泄漏点在后管",
            message_type="text", sent_at=datetime.now(), sender_role="ENGINEER",
        )
        db.enqueue_message(msg)
        row = db.connect().execute(
            "SELECT * FROM inbox_messages WHERE message_id='m1'").fetchone()
        status = await pipeline.process(dict(row))
        assert status == "COMPLETED"
        sugg = db.get_latest_suggestion(tid)
        assert "implicit_match" in sugg["detail"]
        assert sugg["detail"]["implicit_match"]["hit"] is True
        db.close()
