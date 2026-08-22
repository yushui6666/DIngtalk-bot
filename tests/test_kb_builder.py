"""kb_builder 单测：终态工单→案例文档、业务文档切块、CANCELLED 不入库。"""

from __future__ import annotations

import pytest

from db import Database
from qa.kb_builder import (
    build_ticket_case_documents,
    chunk_markdown_document,
    sync_tickets_to_kb,
)


@pytest.fixture()
def db(tmp_path):
    d = Database(tmp_path / "t.db")
    d.init_schema()
    d.upsert_group({"group_id": "G1", "store_name": "测试店",
                    "manager_ids": ["mgr"], "engineer_ids": ["eng"]})
    yield d
    d.close()


def _mk_ticket(db: Database, ticket_no: str, *, status="COMPLETED",
               subject="冷柜", problem="不制冷，压缩机嗡嗡响", sla_days=3,
               with_diagnosis=True, with_repair=True):
    with db.transaction("t") as conn:
        ticket_id = db.insert_ticket({
            "ticket_no": ticket_no, "group_id": "G1", "store_name": "测试店",
            "reporter_id": "mgr", "subject": subject, "location": "后厨",
            "problem_description": problem, "sla_days": sla_days,
            "initial_deadline_at": "2026-08-01 00:00:00",
            "current_deadline_at": "2026-08-04 00:00:00",
            "status": "ACTIVE",
        })
        conn.execute(
            "UPDATE tickets SET status=?, closed_at=? WHERE id=?",
            (status, "2026-08-03 10:00:00", ticket_id),
        )
    with db.transaction("t"):
        if with_diagnosis:
            db.add_diagnosis_version(ticket_id, f"msg-{ticket_no}-d",
                                     ["制冷剂泄漏"], "eng")
        if with_repair:
            db.add_repair_method_version(ticket_id, f"msg-{ticket_no}-r",
                                         "补充制冷剂+清洗冷凝器", None, "eng")
    return ticket_id


class TestTicketCaseDocuments:
    def test_completed_ticket_with_versions_becomes_case(self, db):
        _mk_ticket(db, "W001")
        docs = build_ticket_case_documents(db)
        assert len(docs) == 1
        doc = docs[0]
        assert doc["doc_id"] == "ticket:W001"
        assert doc["source_type"] == "TICKET_CASE"
        # 模板必须包含 现象→原因→处理 三段关键信息
        assert "冷柜" in doc["content"]
        assert "不制冷" in doc["content"]
        assert "制冷剂泄漏" in doc["content"]
        assert "补充制冷剂" in doc["content"]
        assert doc["metadata"]["ticket_no"] == "W001"

    def test_cancelled_ticket_excluded(self, db):
        _mk_ticket(db, "W001", status="CANCELLED")
        assert build_ticket_case_documents(db) == []

    def test_missing_diagnosis_or_repair_excluded(self, db):
        # 未完单（ACTIVE）不算案例；终态但缺诊断/维修方式的也不算
        _mk_ticket(db, "W-ACTIVE", status="ACTIVE")
        _mk_ticket(db, "W-NODIAG", with_diagnosis=False)
        _mk_ticket(db, "W-NOREP", with_repair=False)
        doc_ids = [d["doc_id"] for d in build_ticket_case_documents(db)]
        assert doc_ids == []


class TestMarkdownChunking:
    def test_chunk_by_heading(self):
        md = "# 使用须知\n\n intro\n\n## 一、怎么报修\n在群里发消息。\n\n## 二、关键词\n#报修 建单。"
        chunks = chunk_markdown_document("使用须知", md, max_chars=300)
        assert [c["title"] for c in chunks] == ["使用须知#一、怎么报修", "使用须知#二、关键词"]
        for c in chunks:
            assert len(c["content"]) <= 300 or "\n" in c["content"]

    def test_long_section_split(self):
        section = "字" * 500
        md = f"# 文档\n\n## 长节\n{section}"
        chunks = chunk_markdown_document("文档", md, max_chars=200)
        assert len(chunks) == 3  # 500 字按 200 切 3 块
        assert all(len(c["content"]) <= 200 for c in chunks)


class TestSync:
    def test_sync_insert_then_deactivate(self, db, tmp_path):
        from qa.kb_store import KBStore
        _mk_ticket(db, "W001")
        _mk_ticket(db, "W002", subject="空调", problem="漏水")
        store = KBStore(tmp_path / "kb.db")
        store.init_schema()

        stats = sync_tickets_to_kb(db, store)
        assert stats["inserted"] == 2
        assert store.get_document("ticket:W001")["is_active"] == 1

        # W002 被取消后同步：软删除
        conn = db.connect()
        conn.execute("UPDATE tickets SET status='CANCELLED' WHERE ticket_no='W002'")
        conn.commit()
        stats2 = sync_tickets_to_kb(db, store)
        assert stats2["deactivated"] == ["ticket:W002"]
        assert store.get_document("ticket:W002")["is_active"] == 0
        store.close()
