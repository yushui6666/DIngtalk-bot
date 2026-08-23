"""混合检索器（方案 v2.0 任务 4）：FTS5 关键词 + 向量语义 → RRF 融合。

设计要点：
- FTS 通道：trigram 子串匹配（设备名/故障词精确命中），BM25 排名；
- 向量通道：numpy 归一化余弦全量 KNN（万级文档 <10ms，无需外部索引）；
- 融合：RRF（Reciprocal Rank Fusion）—— 两路排名取倒数加权，
  对分数量纲不敏感，无需调参；
- min_score：向量通道最高余弦相似度低于阈值 → 整体拒答（宁沉默不误导）；
- query_vector 可为 None（嵌入服务不可用时仅走 FTS，功能降级不中断）。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional

import numpy as np

from logger import get_logger

logger = get_logger(__name__)


@dataclass
class RetrievedDocument:
    doc_id: str
    source_type: str
    title: str
    content: str
    score: float                      # RRF 融合分
    vector_score: float = 0.0         # 原始余弦（阈值判断/调试用）
    fts_rank: int = 0                 # FTS 名次（0=未命中）
    vector_rank: int = 0
    metadata: dict = field(default_factory=dict)


def embed_with_client(text: str, client: Any = None) -> Optional[np.ndarray]:
    """用嵌入客户端把文本转向量；未配置/失败返回 None（同步包装，供检索降级）。

    注意：这是同步便捷函数，仅适合已在事件循环外或允许阻塞的调用方；
    pipeline 内请用 AsyncHybridRetriever 或显式 await 客户端。
    """
    if client is None:
        from qa.embeddings import EmbeddingClient
        client = EmbeddingClient()
    if not getattr(client, "is_configured", False):
        return None
    import asyncio
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None
    try:
        if loop is not None:
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                vectors = pool.submit(
                    lambda: asyncio.run(client.embed([text]))).result()
        else:
            vectors = asyncio.run(client.embed([text]))
    except Exception as exc:  # noqa: BLE001 —— 嵌入失败一律降级为 None
        logger.warning("查询嵌入失败，本次仅用 FTS: %s", type(exc).__name__)
        return None
    if not vectors:
        return None
    return np.asarray(vectors[0], dtype=np.float32)


class HybridRetriever:
    """FTS5 + 向量余弦 + RRF 融合检索。"""

    def __init__(
        self,
        store: Any,
        *,
        rrf_k: int = 60,
        min_score: float = 0.30,
        fts_weight: float = 1.0,
        vector_weight: float = 1.0,
    ) -> None:
        self.store = store
        self.rrf_k = rrf_k
        self.min_score = min_score
        self.fts_weight = fts_weight
        self.vector_weight = vector_weight

    # ─────────────────────── FTS 通道 ───────────────────────

    def search_fts(self, query: str, *, limit: int = 10) -> list[RetrievedDocument]:
        """BM25 关键词检索。query 无引号包裹风险由参数化规避。"""
        try:
            rows = self.store.connect().execute(
                "SELECT doc_id, bm25(kb_fts) AS rank FROM kb_fts"
                " WHERE kb_fts MATCH ? ORDER BY rank LIMIT ?",
                (self._fts_query(query), int(limit)),
            ).fetchall()
        except Exception as exc:  # noqa: BLE001 —— FTS 语法异常降级为空
            logger.warning("FTS 检索失败 query=%r: %s", query[:40], type(exc).__name__)
            return []
        hits: list[RetrievedDocument] = []
        for r in rows:
            doc = self.store.get_document(r["doc_id"])
            if doc is None or not doc["is_active"]:
                continue
            hits.append(RetrievedDocument(
                doc_id=doc["doc_id"], source_type=doc["source_type"],
                title=doc["title"], content=doc["content"],
                score=0.0, metadata=doc["metadata"],
            ))
        return hits

    @staticmethod
    def _fts_query(query: str) -> str:
        """把自然语言转成 FTS5 安全查询：去特殊字符，词组化（trigram 按子串命中）。"""
        cleaned = "".join(ch for ch in query if ch.isalnum() or ch.isspace())
        tokens = [t for t in cleaned.split() if t]
        if not tokens:
            return '""'
        return " ".join(f'"{t}"' for t in tokens)

    # ─────────────────────── 向量通道 ───────────────────────

    def search_vector(
        self, query_vector: np.ndarray, *, limit: int = 10
    ) -> list[RetrievedDocument]:
        """归一化余弦 KNN（全量扫描；文档量 ≤ 万级时耗时可忽略）。"""
        q = np.asarray(query_vector, dtype=np.float32)
        qn = np.linalg.norm(q)
        if qn == 0:
            return []
        q = q / qn

        conn = self.store.connect()
        rows = conn.execute(
            "SELECT v.doc_id, v.vector, v.dim FROM kb_vectors v"
            " JOIN kb_documents d ON d.doc_id = v.doc_id"
            " WHERE d.is_active=1"
        ).fetchall()
        scored: list[tuple[float, dict]] = []
        for r in rows:
            vec = np.frombuffer(r["vector"], dtype=np.float32, count=r["dim"])
            norm = np.linalg.norm(vec)
            if norm == 0:
                continue
            cos = float(np.dot(q, vec / norm))
            scored.append((cos, {"doc_id": r["doc_id"], "cos": cos}))
        scored.sort(key=lambda x: x[0], reverse=True)

        hits: list[RetrievedDocument] = []
        for cos, info in scored[: int(limit)]:
            doc = self.store.get_document(info["doc_id"])
            if doc is None:
                continue
            hits.append(RetrievedDocument(
                doc_id=doc["doc_id"], source_type=doc["source_type"],
                title=doc["title"], content=doc["content"],
                score=0.0, vector_score=cos, metadata=doc["metadata"],
            ))
        return hits

    # ─────────────────────── RRF 融合 ───────────────────────

    def retrieve(
        self,
        query: str,
        *,
        query_vector: Optional[np.ndarray] = None,
        limit: int = 4,
    ) -> list[RetrievedDocument]:
        """混合检索：任一通道可用即出结果；均不可用返回空。

        min_score 语义：向量通道最高余弦 < min_score 且 FTS 未命中任何词
        → 视为无可靠依据，返回空（上层据此沉默/拒答）。
        """
        fts_hits = self.search_fts(query, limit=limit * 3)
        vec_hits = (
            self.search_vector(query_vector, limit=limit * 3)
            if query_vector is not None else []
        )

        # 阈值判断：两通道都弱 → 拒答
        best_vec = vec_hits[0].vector_score if vec_hits else 0.0
        if not fts_hits and best_vec < self.min_score:
            return []

        fused: dict[str, RetrievedDocument] = {}
        scores: dict[str, float] = {}
        for rank, h in enumerate(fts_hits, 1):
            scores[h.doc_id] = scores.get(h.doc_id, 0.0) + (
                self.fts_weight / (self.rrf_k + rank))
            h.fts_rank = rank
            fused[h.doc_id] = h
        for rank, h in enumerate(vec_hits, 1):
            scores[h.doc_id] = scores.get(h.doc_id, 0.0) + (
                self.vector_weight / (self.rrf_k + rank))
            h.vector_rank = rank
            if h.doc_id in fused:
                fused[h.doc_id].vector_score = h.vector_score
            else:
                fused[h.doc_id] = h

        results = sorted(
            fused.values(), key=lambda h: scores[h.doc_id], reverse=True
        )[: int(limit)]
        for h in results:
            h.score = scores[h.doc_id]
        logger.info(
            "混合检索 query=%r fts=%d vec=%d best_cos=%.3f -> %d 条",
            query[:24], len(fts_hits), len(vec_hits), best_vec, len(results),
        )
        return results
