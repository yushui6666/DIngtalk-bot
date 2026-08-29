"""retriever 单测：FTS 命中、向量命中、RRF 融合、阈值拒答（静态向量，不调 API）。"""

from __future__ import annotations

import numpy as np
import pytest

from qa.kb_store import KBStore
from qa.retriever import HybridRetriever, RetrievedDocument, embed_with_client


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


def _add(store, doc_id, title, content, seed, metadata=None):
    store.upsert_document(doc_id=doc_id, source_type="TICKET_CASE",
                          title=title, content=content, metadata=metadata or {})
    store.save_embedding(doc_id, _vec(seed))


@pytest.fixture()
def loaded(store):
    _add(store, "ticket:W001", "冷柜不制冷", "冷柜不制冷，压缩机嗡嗡响，制冷剂泄漏，补充制冷剂解决", 1)
    _add(store, "ticket:W002", "冷柜不制冷", "冷柜冷藏不制冷，冷凝器积灰，清洗后恢复", 2)
    _add(store, "ticket:W003", "空调漏水", "空调内机漏水，排水管堵塞，疏通解决", 3)
    return store


class TestFTS:
    def test_keyword_hit(self, loaded):
        r = HybridRetriever(loaded)
        hits = r.search_fts("冷柜不制冷", limit=5)
        assert {h.doc_id for h in hits} >= {"ticket:W001", "ticket:W002"}

    def test_no_hit_empty(self, loaded):
        r = HybridRetriever(loaded)
        assert r.search_fts("完全无关的词组", limit=5) == []


class TestVector:
    def test_knn_nearest(self, loaded):
        r = HybridRetriever(loaded)
        query_vec = _vec(1)  # 与 W001 完全同向
        hits = r.search_vector(query_vec, limit=2)
        assert hits[0].doc_id == "ticket:W001"
        assert hits[0].vector_score == pytest.approx(1.0, abs=1e-4)

    def test_skips_unembedded(self, loaded):
        loaded.upsert_document(doc_id="ticket:W004", source_type="TICKET_CASE",
                               title="x", content="y", metadata={})
        r = HybridRetriever(loaded)
        hits = r.search_vector(_vec(1), limit=10)
        assert "ticket:W004" not in [h.doc_id for h in hits]


class TestHybridRRF:
    def test_rrf_fusion_both_channels(self, loaded):
        r = HybridRetriever(loaded, rrf_k=60)
        # 查询词命中 FTS，向量与 W001 同向 → W001 应排第一
        results = r.retrieve("冷柜不制冷", query_vector=_vec(1), limit=3)
        assert results[0].doc_id == "ticket:W001"

    def test_vector_only_when_no_fts_hit(self, loaded):
        r = HybridRetriever(loaded)
        results = r.retrieve("奇怪的说法", query_vector=_vec(3), limit=2)
        assert results[0].doc_id == "ticket:W003"

    def test_min_score_filter(self, loaded):
        # 正交向量 + 无 FTS 命中 → 相关度极低，min_score 应滤空
        r = HybridRetriever(loaded, min_score=0.9)
        results = r.retrieve("无关词", query_vector=_vec(999), limit=3)
        assert results == []

    def test_returns_metadata(self, loaded):
        loaded.upsert_document(
            doc_id="ticket:W009", source_type="TICKET_CASE",
            title="冷柜不制冷", content="冷柜不制冷反复，更换压缩机",
            metadata={"ticket_no": "W009"})
        loaded.save_embedding("ticket:W009", _vec(1))
        r = HybridRetriever(loaded)
        results = r.retrieve("冷柜不制冷", query_vector=_vec(1), limit=1)
        assert results[0].doc_id == "ticket:W009"
        assert results[0].metadata["ticket_no"] == "W009"


class TestEmbedHelper:
    def test_embed_with_client_not_configured_returns_none(self, monkeypatch):
        monkeypatch.delenv("EMBEDDING_API_KEY", raising=False)
        assert embed_with_client("文本") is None

    def test_embed_with_client_ok(self, monkeypatch):
        import qa.embeddings as emb
        monkeypatch.setenv("EMBEDDING_API_KEY", "sk-test")

        class FakeClient:
            is_configured = True

            async def embed(self, texts):
                return [[0.1, 0.2]]

        orig = emb.EmbeddingClient
        emb.EmbeddingClient = lambda: FakeClient()  # type: ignore
        try:
            vec = embed_with_client("文本")
        finally:
            emb.EmbeddingClient = orig  # type: ignore
        assert vec is not None and vec.shape == (2,)
