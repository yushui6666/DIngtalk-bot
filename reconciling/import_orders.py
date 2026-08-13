"""淘宝对账数据导入（与「淘宝订单自动下载与地址对账」工具衔接）。

把工具产出的 `订单地址明细.xlsx`（order_id → 收货地址/订单状态/物流单号）
导入系统 `taobao_orders` 表，供报修工单在提交订单号时校验 + 快递确认增强。

用法::

    python -m reconciling.import_orders \
        --xlsx "/Users/yushui/Desktop/淘宝对账/订单地址明细.xlsx" \
        [--pending "/Users/yushui/Desktop/淘宝对账/待人工处理.xlsx"]

对账工具每跑一次，重跑本命令即可增量更新（按 order_id 幂等 upsert）。
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

import openpyxl

from db import Database

_PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _load_sheet_rows(path: Path) -> list[dict[str, Any]]:
    """读取 xlsx 首个工作表 → 字典行（表头为准）。"""
    wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    ws = wb.worksheets[0]
    rows = list(ws.iter_rows(values_only=True))
    wb.close()
    if not rows:
        return []
    headers = [str(h).strip() if h is not None else "" for h in rows[0]]
    data: list[dict[str, Any]] = []
    for row in rows[1:]:
        if row is None or all(c is None for c in row):
            continue
        data.append({headers[i]: row[i] for i in range(min(len(headers), len(row)))})
    return data


def import_orders(db: Database, xlsx_path: Path, pending_path: Path | None = None) -> dict[str, int]:
    """导入订单地址明细 + 待人工处理，返回统计。"""
    stats = {"main": 0, "pending": 0, "orders": 0}

    if xlsx_path.exists():
        by_order: dict[str, dict[str, Any]] = {}
        for row in _load_sheet_rows(xlsx_path):
            order_id = str(row.get("order_id") or "").strip()
            if not order_id:
                continue
            agg = by_order.setdefault(order_id, {
                "products": [], "tracking": None, "address": None, "status": None,
            })
            product = str(row.get("product_name") or "").strip()
            qty = row.get("quantity")
            variant = str(row.get("variant") or "").strip()
            agg["products"].append(f"{product}({variant})×{qty}" if variant and qty else product)
            agg["tracking"] = agg["tracking"] or (str(row.get("tracking_number") or "").strip() or None)
            agg["address"] = agg["address"] or (str(row.get("address") or "").strip() or None)
            agg["status"] = agg["status"] or (str(row.get("status") or "").strip() or None)
        for order_id, agg in by_order.items():
            db.upsert_taobao_order(
                order_id=order_id,
                product_summary="；".join(p for p in agg["products"] if p)[:200] or "",
                tracking_number=agg["tracking"],
                address=agg["address"],
                status=agg["status"],
                source="地址明细",
            )
        stats["main"] = len(by_order)
        stats["orders"] = len(by_order)

    if pending_path is not None and pending_path.exists():
        for row in _load_sheet_rows(pending_path):
            order_id = str(row.get("order_id") or "").strip()
            if not order_id:
                continue
            db.upsert_taobao_order(
                order_id=order_id,
                product_summary=str(row.get("product_name") or "")[:200],
                tracking_number=None,
                address=None,
                status="待人工处理",
                source="待人工处理",
            )
            stats["pending"] += 1
            stats["orders"] += 1

    return stats


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="导入淘宝对账订单表")
    parser.add_argument("--xlsx", type=Path, required=True, help="订单地址明细.xlsx")
    parser.add_argument("--pending", type=Path, default=None, help="待人工处理.xlsx（可选）")
    parser.add_argument("--db", type=Path, default=None, help="数据库路径（默认 config.DB_PATH）")
    args = parser.parse_args(argv)

    from config import DB_PATH

    db = Database(args.db or DB_PATH)
    db.init_schema()
    stats = import_orders(db, args.xlsx, args.pending)
    print(f"导入完成：明细订单 {stats['main']}，待人工处理 {stats['pending']}，共 {stats['orders']} 单")
    db.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
