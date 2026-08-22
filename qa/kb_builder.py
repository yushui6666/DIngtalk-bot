"""语料构建（方案 v2.0 任务 2）：把业务数据变成可检索的知识文档。

三类语料：
1. TICKET_CASE：终态成功工单（COMPLETED/STOPPED 且有当前诊断+维修方式）
   → 一张工单 = 一篇"现象→原因→处理"案例文档；
   CANCELLED（误报）与缺诊断/维修方式的工单不入库，避免污染。
2. DOC：业务文档（使用须知/系统说明）按二级标题切块，超长节再按字符切。
3. FAQ（二期）：一问一答一篇。

同步策略：upsert + content_hash 变更检测；语料源消失时软删除（is_active=0）。
"""

from __future__ import annotations

import json
import re
from typing import Any

from logger import get_logger

logger = get_logger(__name__)

# 可入库的工单终态（CANCELLED 误报单不入库）
_CASE_ELIGIBLE_STATUSES = ("COMPLETED", "STOPPED")

_HEADING_RE = re.compile(r"^#{1,6}\s+")


def build_ticket_case_documents(db: Any) -> list[dict[str, Any]]:
    """终态成功工单 → 案例文档列表。

    必须同时具备当前故障判断与当前维修方式，否则不足以构成
    「现象→原因→处理」完整案例，跳过。
    """
    conn = db.connect()
    rows = conn.execute(
        f"""
        SELECT t.id, t.ticket_no, t.store_name, t.subject, t.location,
               t.problem_description, t.sla_days, t.closed_at, t.status
        FROM tickets t WHERE t.status IN (?, ?)
        """,
        _CASE_ELIGIBLE_STATUSES,
    ).fetchall()

    documents: list[dict[str, Any]] = []
    for t in rows:
        diag = conn.execute(
            "SELECT items_json, engineer_id FROM diagnosis_versions"
            " WHERE ticket_id=? AND is_current=1", (t["id"],),
        ).fetchone()
        repair = conn.execute(
            "SELECT repair_method, order_no FROM repair_method_versions"
            " WHERE ticket_id=? AND is_current=1", (t["id"],),
        ).fetchone()
        if diag is None or repair is None:
            continue
        try:
            items = json.loads(diag["items_json"] or "[]")
        except (TypeError, ValueError):
            items = []

        content = (
            f"【维修案例】{t['subject']}——{t['problem_description']}"
            f"（工单 {t['ticket_no']}，{t['status']}）\n"
            f"门店/空间：{t['store_name']} · {t['location']}\n"
            f"故障描述：{t['problem_description']}\n"
            f"故障判断：{'；'.join(str(x) for x in items)}\n"
            f"维修方式：{repair['repair_method']}"
            + (f"（订单 {repair['order_no']}）" if repair["order_no"] else "")
            + "\n"
            f"时效：{t['sla_days']}天 · 完成：{t['closed_at'] or '-'}"
        )
        documents.append({
            "doc_id": f"ticket:{t['ticket_no']}",
            "source_type": "TICKET_CASE",
            "title": f"{t['subject']}·{t['problem_description'][:24]}",
            "content": content,
            "metadata": {
                "ticket_no": t["ticket_no"],
                "group_store": t["store_name"],
                "subject": t["subject"],
                "location": t["location"],
                "status": t["status"],
            },
        })
    logger.info("工单案例构建完成 cases=%d / 终态工单=%d", len(documents), len(rows))
    return documents


def chunk_markdown_document(
    doc_name: str, markdown: str, *, max_chars: int = 300,
) -> list[dict[str, Any]]:
    """Markdown 按二级及以下标题切块；超长节按 max_chars 二次切分。"""
    sections: list[tuple[str, str]] = []
    current_title = ""
    buf: list[str] = []

    def flush() -> None:
        if buf and current_title:
            sections.append((current_title, "\n".join(buf).strip()))

    for line in markdown.splitlines():
        if _HEADING_RE.match(line):
            flush()
            level = len(line) - len(line.lstrip("#"))
            # H1 是文档标题本身：其后、首个二级标题前的前言不入块
            current_title = _HEADING_RE.sub("", line).strip() if level >= 2 else ""
            buf = []
        elif current_title:
            buf.append(line)
    flush()

    chunks: list[dict[str, Any]] = []
    for title, body in sections:
        if not body:
            continue
        pieces = _split_long(body, max_chars)
        for i, piece in enumerate(pieces):
            suffix = f"({i + 1}/{len(pieces)})" if len(pieces) > 1 else ""
            chunk_title = f"{doc_name}#{title}{suffix}"
            chunks.append({
                "doc_id": _doc_id_for(chunk_title),
                "source_type": "DOC",
                "title": chunk_title,
                "content": f"{title}\n{piece}" if len(pieces) == 1 else piece,
                "metadata": {"doc": doc_name, "section": title},
            })
    return chunks


def _split_long(text: str, max_chars: int) -> list[str]:
    if len(text) <= max_chars:
        return [text]
    pieces, start = [], 0
    while start < len(text):
        pieces.append(text[start:start + max_chars])
        start += max_chars
    return pieces


def _doc_id_for(title: str) -> str:
    safe = re.sub(r"[\s#/\\]+", "-", title).strip("-")
    return f"doc:{safe}"


def sync_tickets_to_kb(db: Any, store: Any) -> dict[str, Any]:
    """工单案例 → 知识库增量同步（upsert + 软删除）。"""
    docs = build_ticket_case_documents(db)
    inserted = updated = unchanged = 0
    for doc in docs:
        result = store.upsert_document(
            doc_id=doc["doc_id"], source_type=doc["source_type"],
            title=doc["title"], content=doc["content"], metadata=doc["metadata"],
        )
        if result == "INSERTED":
            inserted += 1
        elif result == "UPDATED":
            updated += 1
        else:
            unchanged += 1
    deactivated = store.deactivate_missing(d["doc_id"] for d in docs)
    stats = {"inserted": inserted, "updated": updated,
             "unchanged": unchanged, "deactivated": deactivated}
    logger.info("知识库工单同步完成 %s", stats)
    return stats
