"""知识库构建脚本（v4.3 任务 8）。

用法::

    # 全量同步工单案例 + 批量嵌入（需 EMBEDDING_API_KEY）
    python scripts/build_kb.py

    # 只同步文档不嵌入（FTS 检索可用，向量通道关闭）
    python scripts/build_kb.py --no-embed

    # 指定业务库路径
    python scripts/build_kb.py --db data/tickets.db
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from db import Database  # noqa: E402
from qa.sync import sync_knowledge_base  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="构建 RAG 知识库")
    parser.add_argument("--db", default=None, help="业务库路径（默认 config.DB_PATH）")
    parser.add_argument("--no-embed", action="store_true", help="只同步文档，不调用嵌入")
    args = parser.parse_args()

    db = Database(args.db) if args.db else Database()
    db.init_schema()
    stats = sync_knowledge_base(db, embed=not args.no_embed)
    db.close()
    print(f"知识库同步完成: {stats}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
