"""RAG 闭环顾问指标报表（v4.3 任务 8）。

四项核心指标（方案 §7）::
    python scripts/qa_metrics.py [--db data/tickets.db] [--days 30]

    建议覆盖率 = 发出建议的工单 / 总建单
    自助解决率 = 「解决了」完单且诊断为 AI 的建议工单 / 发出建议的工单
    升级率     = 「未解决」升级的建议工单 / 发出建议的工单
    隐式命中率 = 工程师诊断与建议原因吻合的建议工单 / 有隐式比对的建议工单
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from db import Database  # noqa: E402


def compute_metrics(db: Database, days: int | None = None) -> dict:
    conn = db.connect()
    since_clause = ""
    params: list = []
    if days:
        since_clause = " AND created_at >= datetime('now', ?)"
        params.append(f"-{int(days)} days")

    total_tickets = conn.execute(
        f"SELECT COUNT(*) AS c FROM tickets WHERE 1=1{since_clause}", params
    ).fetchone()["c"]

    suggestions = conn.execute(
        f"""SELECT s.id, s.ticket_id, s.feedback, s.escalated_at, s.detail,
                   (SELECT engineer_id FROM diagnosis_versions dv
                    WHERE dv.ticket_id = s.ticket_id AND dv.is_current = 1
                    AND dv.engineer_id = 'AI') AS ai_diag
            FROM ticket_suggestions s JOIN tickets t ON t.id = s.ticket_id
            WHERE 1=1{since_clause.replace('created_at', 't.created_at')}""",
        params,
    ).fetchall()

    advised = len(suggestions)
    resolved_self = sum(
        1 for s in suggestions if s["feedback"] == "RESOLVED" and s["ai_diag"])
    escalated = sum(1 for s in suggestions if s["escalated_at"])
    implicit_total = implicit_hit = 0
    for s in suggestions:
        try:
            detail = json.loads(s["detail"] or "{}")
        except (TypeError, ValueError):
            continue
        im = detail.get("implicit_match")
        if im is not None:
            implicit_total += 1
            if im.get("hit"):
                implicit_hit += 1

    def rate(num: int, den: int) -> float:
        return round(num / den, 3) if den else 0.0

    return {
        "window_days": days or "all",
        "total_tickets": total_tickets,
        "advised_tickets": advised,
        "coverage_rate": rate(advised, total_tickets),
        "self_resolution_count": resolved_self,
        "self_resolution_rate": rate(resolved_self, advised),
        "escalated_count": escalated,
        "escalation_rate": rate(escalated, advised),
        "implicit_compared": implicit_total,
        "implicit_hit_rate": rate(implicit_hit, implicit_total),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="RAG 闭环指标报表")
    parser.add_argument("--db", default=None)
    parser.add_argument("--days", type=int, default=None, help="统计窗口（天）")
    args = parser.parse_args()

    db = Database(args.db) if args.db else Database()
    db.init_schema()
    m = compute_metrics(db, args.days)
    db.close()

    print(f"RAG 闭环指标（窗口={m['window_days']}）")
    print(f"  建议覆盖率   {m['coverage_rate']:>6.1%}  ({m['advised_tickets']}/{m['total_tickets']} 工单)")
    print(f"  自助解决率   {m['self_resolution_rate']:>6.1%}  ({m['self_resolution_count']}/{m['advised_tickets']} 建议)")
    print(f"  升级率       {m['escalation_rate']:>6.1%}  ({m['escalated_count']}/{m['advised_tickets']} 建议)")
    print(f"  隐式命中率   {m['implicit_hit_rate']:>6.1%}  ({m['implicit_compared']} 次比对)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
