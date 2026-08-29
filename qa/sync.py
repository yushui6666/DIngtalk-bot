"""知识库同步入口（v4.3 任务 8）。

被调度器周期调用（scan_kb_sync）与 scripts/build_kb.py 手动调用。
工单案例 upsert → 待嵌入文档批量嵌入（嵌入服务未配置时跳过，
FTS 检索仍可用——建议链路功能降级不中断）。
"""

from __future__ import annotations

from typing import Any

from logger import get_logger

logger = get_logger(__name__)


def sync_knowledge_base(db: Any, store: Any = None, *, embed: bool = True) -> dict[str, Any]:
    """工单案例 → 知识库增量同步 + 待嵌入批量嵌入。

    Args:
        db: 业务库（工单真相源）。
        store: 知识库存储；None 时用与主库同文件的 KBStore。
        embed: 是否执行嵌入（未配置 EMBEDDING_API_KEY 时自动跳过）。

    Returns:
        {"inserted", "updated", "unchanged", "deactivated", "embedded"}
    """
    if store is None:
        from config import DB_PATH
        from qa.kb_store import KBStore

        store = KBStore(DB_PATH)
        store.init_schema()

    from qa.kb_builder import sync_tickets_to_kb

    stats = sync_tickets_to_kb(db, store)

    if embed:
        stats["embedded"] = embed_pending(store)
    else:
        stats["embedded"] = 0
    return stats


def embed_pending(store: Any, *, limit: int = 256) -> int:
    """批量嵌入待办文档；未配置嵌入服务返回 0。"""
    pending = store.pending_embeddings(limit=limit)
    if not pending:
        return 0
    from qa.embeddings import EmbeddingClient

    client = EmbeddingClient()
    if not client.is_configured:
        logger.info("嵌入服务未配置，跳过 %d 条待嵌入文档", len(pending))
        return 0

    import asyncio

    texts = [f"{p['title']} {p['content']}" for p in pending]
    vectors = asyncio.run(client.embed(texts))
    for doc, vec in zip(pending, vectors):
        store.save_embedding(doc["doc_id"], __import__("numpy").asarray(vec, dtype="float32"))
    logger.info("批量嵌入完成 count=%d", len(pending))
    return len(pending)
