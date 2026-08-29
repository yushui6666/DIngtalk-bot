"""任务 8：知识库同步、指标报表、群白名单。"""

from __future__ import annotations

import json

import pytest

from db import Database
from qa.advisor import TicketAdvisor
from qa.kb_store import KBStore
from qa.sync import sync_knowledge_base


@pytest.fixture()
def env(tmp_path):
    db = Database(tmp_path / "t.db")
    db.init_schema()
    db.upsert_group({"group_id": "G1", "store_name": "店A",
                     "manager_ids": ["m"], "engineer_ids": ["e"]})
    store = KBStore(tmp_path / "kb.db")
    store.init_schema()
    yield db, store
    db.close()
    store.close()


def _mk_case_ticket(db, no, *, status="COMPLETED"):
    with db.transaction("t") as conn:
        tid = db.insert_ticket({
            "ticket_no": no, "group_id": "G1", "store_name": "店A",
            "reporter_id": "m", "subject": "冷柜", "location": "后厨",
            "problem_description": "不制冷", "sla_days": 3,
            "initial_deadline_at": "2026-08-01 00:00:00",
            "current_deadline_at": "2026-08-04 00:00:00",
            "status": "ACTIVE",
        })
        conn.execute("UPDATE tickets SET status=?, closed_at=? WHERE id=?",
                     (status, "2026-08-03 10:00:00", tid))
    db.add_diagnosis_version(tid, f"msg-{no}-d", ["制冷剂泄漏"], "e")
    db.add_repair_method_version(tid, f"msg-{no}-r", "补充制冷剂", None, "e")
    return tid


class TestSync:
    def test_sync_and_stats(self, env):
        db, store = env
        _mk_case_ticket(db, "W001")
        stats = sync_knowledge_base(db, store, embed=False)
        assert stats["inserted"] == 1
        assert stats["embedded"] == 0
        assert store.get_document("ticket:W001") is not None

        # 幂等：再同步无插入
        stats2 = sync_knowledge_base(db, store, embed=False)
        assert stats2["inserted"] == 0 and stats2["unchanged"] == 1


class TestMetrics:
    def test_metrics_compute(self, env):
        from scripts.qa_metrics import compute_metrics
        db, store = env
        # 工单1：建议 + AI自助解决
        tid1 = _mk_case_ticket(db, "W001")
        db.record_suggestion(tid1, ["ticket:X"], 0.9, "建议",
                             detail={"causes": ["制冷剂泄漏"], "repairs": ["补充制冷剂"]})
        # 工单2：建议 + 升级
        tid2 = _mk_case_ticket(db, "W002")
        db.record_suggestion(tid2, ["ticket:Y"], 0.85, "建议2",
                             detail={"causes": ["积灰"], "repairs": ["清洗"]})
        db.mark_suggestion_escalated(tid2)
        # 工单3：无建议
        _mk_case_ticket(db, "W003")

        m = compute_metrics(db)
        assert m["total_tickets"] == 3
        assert m["advised_tickets"] == 2
        assert m["coverage_rate"] == round(2 / 3, 3)
        assert m["escalated_count"] == 1
        assert m["escalation_rate"] == 0.5


class TestWhitelist:
    def test_whitelist_blocks_other_group(self, env):
        db, store = env
        ticket = {"id": 1, "ticket_no": "W001", "group_id": "G9",
                  "subject": "冷柜", "location": "后厨",
                  "problem_description": "不制冷"}
        advisor = TicketAdvisor(db, store, group_whitelist={"G1"})
        assert advisor.advise_for_new_ticket(ticket) is None
