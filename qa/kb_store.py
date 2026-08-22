"""知识库存储层（方案 v2.0 任务 1）。

设计要点：
- 与主业务库同文件（SQLite 唯一真相源），独立连接（WAL + busy_timeout）；
- kb_documents：一篇案例/文档块一行，正文与元数据分离，content_hash 变更检测；
- kb_fts：FTS5 trigram 虚表（中文子串匹配，无需分词器依赖）；
- kb_vectors：向量 BLOB 存储 + numpy 余弦（本系统规模 ≤ 万级文档，
  纯 numpy KNN <10ms，省去 sqlite-vec 原生扩展在生产机的加载风险；
  若未来文档量上十万级，可平滑切换 vec0 虚表——接口不变）；
- 嵌入生命周期：内容变更 → content_hash 变化 → embedded_at 置空 +
  旧向量作废 → pending_embeddings() 重新出队。
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from typing import Any, Iterable, Optional

import numpy as np

from logger import get_logger

logger = get_logger(__name__)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS kb_documents (
    doc_id       TEXT PRIMARY KEY,   -- ticket:W001 / doc:使用须知#2 / faq:0001
    source_type  TEXT NOT NULL,      -- TICKET_CASE / DOC / FAQ
    title        TEXT NOT NULL,
    content      TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    metadata     TEXT NOT NULL DEFAULT '{}',
    embedded_at  TEXT,
    is_active    INTEGER NOT NULL DEFAULT 1,
    updated_at   TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_kbdoc_active ON kb_documents(is_active);

CREATE TABLE IF NOT EXISTS kb_vectors (
    doc_id  TEXT PRIMARY KEY,
    dim     INTEGER NOT NULL,
    vector  BLOB NOT NULL
);

-- trigram：SQLite 3.34+ 内建，支持中文子串与 LIKE 加速
CREATE VIRTUAL TABLE IF NOT EXISTS kb_fts USING fts5(
    doc_id UNINDEXED,
    title,
    content,
    tokenize='trigram'
);
"""


def _now_str() -> str:
    from datetime import datetime
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _content_hash(content: str) -> str:
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


class KBStore:
    """知识文档 + FTS + 向量 的持久化。"""

    def __init__(self, db_path: Any) -> None:
        self._path = str(db_path)
        self._conn: Optional[sqlite3.Connection] = None

    # ─────────────────────── 连接与建表 ───────────────────────

    def connect(self) -> sqlite3.Connection:
        if self._conn is None:
            conn = sqlite3.connect(self._path, timeout=30)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA busy_timeout=30000")
            self._conn = conn
        return self._conn

    def init_schema(self) -> None:
        """幂等建表（executescript 自动提交，不包事务）。"""
        self.connect().executescript(_SCHEMA)
        logger.info("知识库 schema 就绪 path=%s", self._path)

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    # ─────────────────────── 文档 CRUD ───────────────────────

    def upsert_document(
        self,
        *,
        doc_id: str,
        source_type: str,
        title: str,
        content: str,
        metadata: Optional[dict] = None,
    ) -> str:
        """写入/更新文档，按 content_hash 检测变更。

        Returns:
            INSERTED / UPDATED / UNCHANGED
        """
        new_hash = _content_hash(content)
        meta_json = json.dumps(metadata or {}, ensure_ascii=False)
        conn = self.connect()
        row = conn.execute(
            "SELECT content_hash, embedded_at FROM kb_documents WHERE doc_id=?",
            (doc_id,),
        ).fetchone()
        try:
            if row is None:
                conn.execute(
                    "INSERT INTO kb_documents "
                    "(doc_id, source_type, title, content, content_hash, metadata,"
                    " embedded_at, is_active, updated_at)"
                    " VALUES (?,?,?,?,?,?,NULL,1,?)",
                    (doc_id, source_type, title, content, new_hash, meta_json, _now_str()),
                )
                self._fts_upsert(conn, doc_id, title, content)
                conn.commit()
                return "INSERTED"

            if row["content_hash"] == new_hash:
                # 内容未变：若曾被 deactivate，恢复激活
                conn.execute(
                    "UPDATE kb_documents SET is_active=1 WHERE doc_id=?", (doc_id,))
                conn.commit()
                return "UNCHANGED"

            conn.execute(
                "UPDATE kb_documents SET source_type=?, title=?, content=?,"
                " content_hash=?, metadata=?, embedded_at=NULL, is_active=1,"
                " updated_at=? WHERE doc_id=?",
                (source_type, title, content, new_hash, meta_json, _now_str(), doc_id),
            )
            conn.execute("DELETE FROM kb_vectors WHERE doc_id=?", (doc_id,))
            self._fts_upsert(conn, doc_id, title, content)
            conn.commit()
            return "UPDATED"
        except Exception:
            conn.rollback()
            raise

    @staticmethod
    def _fts_upsert(conn: sqlite3.Connection, doc_id: str, title: str, content: str) -> None:
        conn.execute("DELETE FROM kb_fts WHERE doc_id=?", (doc_id,))
        conn.execute(
            "INSERT INTO kb_fts (doc_id, title, content) VALUES (?,?,?)",
            (doc_id, title, content),
        )

    def get_document(self, doc_id: str) -> Optional[dict]:
        row = self.connect().execute(
            "SELECT * FROM kb_documents WHERE doc_id=?", (doc_id,)
        ).fetchone()
        if row is None:
            return None
        d = dict(row)
        d["metadata"] = json.loads(d["metadata"] or "{}")
        return d

    def active_documents(self) -> list[dict]:
        rows = self.connect().execute(
            "SELECT doc_id, source_type, title, metadata FROM kb_documents"
            " WHERE is_active=1 ORDER BY doc_id"
        ).fetchall()
        return [
            {**dict(r), "metadata": json.loads(r["metadata"] or "{}")}
            for r in rows
        ]

    def deactivate_missing(self, active_doc_ids: Iterable[str]) -> list[str]:
        """语料同步后，把不再存在的文档停用（软删除，保留历史）。"""
        keep = set(active_doc_ids)
        rows = self.connect().execute(
            "SELECT doc_id FROM kb_documents WHERE is_active=1"
        ).fetchall()
        removed = [r["doc_id"] for r in rows if r["doc_id"] not in keep]
        if removed:
            conn = self.connect()
            try:
                for doc_id in removed:
                    conn.execute(
                        "UPDATE kb_documents SET is_active=0 WHERE doc_id=?", (doc_id,))
                conn.commit()
            except Exception:
                conn.rollback()
                raise
        return removed

    # ─────────────────────── 嵌入管理 ───────────────────────

    def save_embedding(self, doc_id: str, vector: np.ndarray) -> None:
        vec = np.asarray(vector, dtype=np.float32)
        conn = self.connect()
        try:
            conn.execute(
                "INSERT OR REPLACE INTO kb_vectors (doc_id, dim, vector) VALUES (?,?,?)",
                (doc_id, int(vec.shape[0]), vec.tobytes()),
            )
            conn.execute(
                "UPDATE kb_documents SET embedded_at=? WHERE doc_id=?",
                (_now_str(), doc_id),
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    def get_embedding(self, doc_id: str) -> Optional[np.ndarray]:
        row = self.connect().execute(
            "SELECT dim, vector FROM kb_vectors WHERE doc_id=?", (doc_id,)
        ).fetchone()
        if row is None:
            return None
        return np.frombuffer(row["vector"], dtype=np.float32, count=row["dim"])

    def pending_embeddings(self, limit: Optional[int] = None) -> list[dict]:
        """待嵌入文档：激活且 embedded_at 为空。"""
        sql = (
            "SELECT doc_id, title, content FROM kb_documents"
            " WHERE is_active=1 AND embedded_at IS NULL ORDER BY updated_at"
        )
        if limit is not None:
            sql += f" LIMIT {int(limit)}"
        return [dict(r) for r in self.connect().execute(sql).fetchall()]
