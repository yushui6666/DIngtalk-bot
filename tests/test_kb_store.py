"""kb_store 单测：建表幂等、content_hash 变更检测、嵌入待办管理。"""

from __future__ import annotations

import numpy as np
import pytest

from qa.kb_store import KBStore


@pytest.fixture()
def store(tmp_path):
    s = KBStore(tmp_path / "kb.db")
    s.init_schema()
    yield s
    s.close()


def _upsert(store, doc_id, content="冷柜不制冷，压缩机嗡嗡响", title="冷柜不制冷"):
    return store.upsert_document(
        doc_id=doc_id, source_type="TICKET_CASE", title=title,
        content=content, metadata={"ticket_no": "W001"},
    )


class TestSchema:
    def test_init_schema_idempotent(self, store, tmp_path):
        store.init_schema()  # 重复初始化不报错
        again = KBStore(tmp_path / "kb.db")
        again.init_schema()
        again.close()

    def test_tables_created(self, store):
        names = {
            r["name"] for r in store.connect().execute(
                "SELECT name FROM sqlite_master WHERE type IN ('table','view') "
                "AND name NOT LIKE 'sqlite_%'"
            )
        }
        assert {"kb_documents", "kb_fts", "kb_vectors"} <= names


class TestUpsertAndHash:
    def test_insert_new_document(self, store):
        result = _upsert(store, "ticket:W001")
        assert result == "INSERTED"
        row = store.get_document("ticket:W001")
        assert row["title"] == "冷柜不制冷"
        assert row["is_active"] == 1
        assert row["content_hash"]
        assert row["embedded_at"] is None

    def test_same_content_no_change(self, store):
        _upsert(store, "ticket:W001")
        assert _upsert(store, "ticket:W001") == "UNCHANGED"

    def test_changed_content_updates_hash_and_clears_embedding(self, store):
        _upsert(store, "ticket:W001")
        store.save_embedding("ticket:W001", np.array([0.1, 0.2], dtype=np.float32))
        assert store.get_document("ticket:W001")["embedded_at"] is not None

        result = _upsert(store, "ticket:W001", content="冷柜完全不制冷了")
        assert result == "UPDATED"
        row = store.get_document("ticket:W001")
        assert row["embedded_at"] is None          # 需要重新嵌入
        assert store.get_embedding("ticket:W001") is None  # 旧向量作废

    def test_metadata_persisted_as_json(self, store):
        _upsert(store, "ticket:W001")
        row = store.get_document("ticket:W001")
        assert row["metadata"]["ticket_no"] == "W001"


class TestEmbeddingPipeline:
    def test_save_and_get_embedding_roundtrip(self, store):
        _upsert(store, "ticket:W001")
        vec = np.array([0.5, -0.5, 1.0], dtype=np.float32)
        store.save_embedding("ticket:W001", vec)
        loaded = store.get_embedding("ticket:W001")
        assert np.allclose(loaded, vec)

    def test_pending_embeddings_excludes_embedded(self, store):
        _upsert(store, "ticket:W001")
        _upsert(store, "ticket:W002", content="空调漏水")
        store.save_embedding("ticket:W001", np.zeros(3, dtype=np.float32))
        pending = store.pending_embeddings()
        assert [p["doc_id"] for p in pending] == ["ticket:W002"]

    def test_pending_embeddings_respects_limit(self, store):
        for i in range(5):
            _upsert(store, f"ticket:W{i:03d}", content=f"故障{i}")
        assert len(store.pending_embeddings(limit=3)) == 3


class TestActiveManagement:
    def test_deactivate_missing(self, store):
        _upsert(store, "ticket:W001")
        _upsert(store, "ticket:W002", content="空调漏水")
        # 同步后只剩 W001：W002 应被停用
        removed = store.deactivate_missing({"ticket:W001"})
        assert removed == ["ticket:W002"]
        assert store.get_document("ticket:W002")["is_active"] == 0
        assert store.get_document("ticket:W001")["is_active"] == 1

    def test_active_documents_only_active(self, store):
        _upsert(store, "ticket:W001")
        _upsert(store, "ticket:W002", content="空调漏水")
        store.deactivate_missing({"ticket:W001"})
        docs = store.active_documents()
        assert [d["doc_id"] for d in docs] == ["ticket:W001"]
