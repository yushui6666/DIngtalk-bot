"""SQLite 数据层：9 张表、索引、唯一约束与事务封装。

表结构对应计划书第 15 节（v3.0 基线 → v4.0 迁移中）：
  groups / tickets / responsibility_cycles / messages / processed_events /
  diagnosis_versions / repair_method_versions / timeout_cycles / notification_deliveries

关键约束：
- tickets: ⚠️ v3.0 遗留——每群最多一张非终态工单（部分唯一索引，
  WHERE status IN (ACTIVE, ACTIVE_OVERDUE)）。v4.0 Task 5 将删除此索引并新增
  inbox_messages / pending_actions / action_confirmations / ticket_routing 表。
  在 Task 5 完成前，同群创建多张活动工单会导致 UNIQUE constraint 失败。
- messages.message_id 唯一；processed_events.message_id 主键
- diagnosis/repair_method 的 source_message_id 唯一（一条消息只产生一个版本）
- notification_deliveries.dedupe_key 唯一（Outbox 防重复）

所有业务状态变更必须走 :meth:`Database.transaction`，保证同一事务内
写业务状态 + 写 processed_events + 预写通知 Outbox。
"""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Iterator, Optional

from config import DB_PATH
from logger import get_logger
from models import TICKET_ACTIVE, TICKET_OVERDUE

logger = get_logger(__name__)

_SCHEMA = """
-- ─────────────────────── groups ───────────────────────
CREATE TABLE IF NOT EXISTS groups (
    group_id               TEXT PRIMARY KEY,
    store_name             TEXT NOT NULL,
    manager_ids            TEXT NOT NULL DEFAULT '[]',
    engineer_ids           TEXT NOT NULL DEFAULT '[]',
    other_member_ids       TEXT NOT NULL DEFAULT '[]',
    engineering_leader_id  TEXT NOT NULL DEFAULT '',
    regional_manager_id    TEXT NOT NULL DEFAULT '',
    current_ticket_id      INTEGER,
    ticket_seq             INTEGER NOT NULL DEFAULT 0,
    is_active              INTEGER NOT NULL DEFAULT 1
);

-- ─────────────────────── tickets ───────────────────────
CREATE TABLE IF NOT EXISTS tickets (
    id                             INTEGER PRIMARY KEY AUTOINCREMENT,
    ticket_no                      TEXT NOT NULL UNIQUE,
    group_id                       TEXT NOT NULL,
    store_name                     TEXT NOT NULL,
    reporter_id                    TEXT NOT NULL,
    subject                        TEXT NOT NULL,
    location                       TEXT NOT NULL,
    problem_description            TEXT NOT NULL,
    sla_days                       INTEGER NOT NULL,
    initial_deadline_at            TEXT NOT NULL,
    current_deadline_at            TEXT NOT NULL,
    current_timeout_cycle_id       INTEGER,
    status                         TEXT NOT NULL DEFAULT 'ACTIVE',
    waiting_side                   TEXT NOT NULL DEFAULT 'NONE',
    waiting_since                  TEXT,
    current_responsibility_cycle_id INTEGER,
    last_business_event_at         TEXT,
    last_business_message_id       TEXT,
    created_at                     TEXT NOT NULL,
    closed_at                      TEXT
);

-- ⚠️ v3.0 遗留约束：每群最多一张非终态工单。
-- v4.0 Task 5 将删除此索引以支持同群多工单并行。
-- 删除前，同群创建多张活动工单会触发 UNIQUE constraint 失败。
CREATE UNIQUE INDEX IF NOT EXISTS idx_tickets_one_active
    ON tickets(group_id) WHERE status IN ('ACTIVE', 'ACTIVE_OVERDUE');

CREATE INDEX IF NOT EXISTS idx_tickets_group ON tickets(group_id);
CREATE INDEX IF NOT EXISTS idx_tickets_status ON tickets(status);

-- ─────────────────────── responsibility_cycles ───────────────────────
CREATE TABLE IF NOT EXISTS responsibility_cycles (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    ticket_id          INTEGER NOT NULL,
    waiting_side       TEXT NOT NULL,
    trigger_message_id TEXT NOT NULL,
    waiting_since      TEXT NOT NULL,
    due_at             TEXT,
    status             TEXT NOT NULL DEFAULT 'PENDING',
    claimed_at         TEXT,
    closed_by_message_id TEXT
);

CREATE INDEX IF NOT EXISTS idx_cycles_ticket ON responsibility_cycles(ticket_id);
CREATE INDEX IF NOT EXISTS idx_cycles_status_due ON responsibility_cycles(status, due_at);

-- ─────────────────────── messages ───────────────────────
CREATE TABLE IF NOT EXISTS messages (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    ticket_id    INTEGER NOT NULL,
    message_id   TEXT NOT NULL UNIQUE,
    sender_id    TEXT NOT NULL,
    sender_role  TEXT NOT NULL,
    content      TEXT NOT NULL DEFAULT '',
    message_type TEXT NOT NULL DEFAULT 'text',
    sent_at      TEXT NOT NULL,
    raw_event    TEXT
);

CREATE INDEX IF NOT EXISTS idx_messages_ticket ON messages(ticket_id);

-- ─────────────────────── processed_events ───────────────────────
CREATE TABLE IF NOT EXISTS processed_events (
    message_id  TEXT PRIMARY KEY,
    group_id    TEXT NOT NULL,
    received_at TEXT NOT NULL,
    result      TEXT NOT NULL
);

-- ─────────────────────── diagnosis_versions ───────────────────────
CREATE TABLE IF NOT EXISTS diagnosis_versions (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    ticket_id         INTEGER NOT NULL,
    source_message_id TEXT NOT NULL UNIQUE,
    items_json        TEXT NOT NULL,
    engineer_id       TEXT NOT NULL,
    submitted_at      TEXT NOT NULL,
    is_current        INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_diagnosis_ticket ON diagnosis_versions(ticket_id);

-- ─────────────────────── repair_method_versions ───────────────────────
CREATE TABLE IF NOT EXISTS repair_method_versions (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    ticket_id         INTEGER NOT NULL,
    source_message_id TEXT NOT NULL UNIQUE,
    repair_method     TEXT NOT NULL,
    order_no          TEXT,
    engineer_id       TEXT NOT NULL,
    submitted_at      TEXT NOT NULL,
    is_current        INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_method_ticket ON repair_method_versions(ticket_id);

-- ─────────────────────── timeout_cycles ───────────────────────
CREATE TABLE IF NOT EXISTS timeout_cycles (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    ticket_id          INTEGER NOT NULL,
    cycle_no           INTEGER NOT NULL,
    status             TEXT NOT NULL DEFAULT 'WAITING_REASON',
    old_deadline_at    TEXT NOT NULL,
    reminded_at        TEXT NOT NULL,
    reason             TEXT,
    reason_engineer_id TEXT,
    reason_submitted_at TEXT,
    new_deadline_at    TEXT
);

CREATE INDEX IF NOT EXISTS idx_timeout_ticket ON timeout_cycles(ticket_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_timeout_one_waiting
    ON timeout_cycles(ticket_id) WHERE status = 'WAITING_REASON';

-- ─────────────────────── notification_deliveries ───────────────────────
CREATE TABLE IF NOT EXISTS notification_deliveries (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    dedupe_key        TEXT NOT NULL UNIQUE,
    ticket_id         INTEGER,
    notification_type TEXT NOT NULL,
    target_type       TEXT NOT NULL,
    target_id         TEXT NOT NULL,
    scheduled_at      TEXT,
    sent_at           TEXT,
    status            TEXT NOT NULL DEFAULT 'PENDING',
    attempts          INTEGER NOT NULL DEFAULT 0,
    last_error        TEXT
);

CREATE INDEX IF NOT EXISTS idx_notify_status ON notification_deliveries(status, scheduled_at);
"""


def _now_str() -> str:
    # TODO(v4.0) 应与 ordering.TZ (Asia/Shanghai) 对齐为 aware datetime，
    # 当前 datetime.now() 为 naive，与 ordering 模块的时区感知时间
    # 混用可能导致跨模块时间比较错误。
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


class Database:
    """SQLite 连接与事务封装。单进程 asyncio 下串行使用。"""

    def __init__(self, db_path: Path | str = DB_PATH) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn: Optional[sqlite3.Connection] = None

    # ─────────────────────── 连接与建表 ───────────────────────
    def connect(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = sqlite3.connect(
                str(self.db_path),
                check_same_thread=False,
            )
            self._conn.row_factory = sqlite3.Row
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA foreign_keys=ON")
            logger.info("数据库连接建立 path=%s", self.db_path)
        return self._conn

    def init_schema(self) -> None:
        conn = self.connect()
        with self.transaction("init_schema"):
            conn.executescript(_SCHEMA)
        # 统计表数量做校验
        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        ).fetchall()
        logger.info(
            "数据库 schema 就绪 tables=%d (%s)",
            len(tables),
            ",".join(r["name"] for r in tables),
        )

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None
            logger.info("数据库连接关闭 path=%s", self.db_path)

    # ─────────────────────── 事务封装 ───────────────────────
    @contextmanager
    def transaction(self, tx_name: str = "tx") -> Iterator[sqlite3.Connection]:
        """事务上下文：提交成功打 INFO，回滚打 ERROR。

        业务状态 + processed_events + 通知 Outbox 必须同事务写入。
        """
        conn = self.connect()
        logger.debug("事务开始 tx=%s", tx_name)
        try:
            conn.execute("BEGIN IMMEDIATE")
            yield conn
        except Exception as exc:
            conn.rollback()
            logger.error("事务回滚 tx=%s reason=%s", tx_name, exc)
            raise
        else:
            conn.commit()
            logger.info("事务提交 tx=%s", tx_name)

    # ─────────────────────── 基础查询 ───────────────────────
    def get_group(self, group_id: str) -> Optional[dict[str, Any]]:
        conn = self.connect()
        row = conn.execute("SELECT * FROM groups WHERE group_id=?", (group_id,)).fetchone()
        if row is None:
            return None
        d = dict(row)
        d["manager_ids"] = json.loads(d.get("manager_ids") or "[]")
        d["engineer_ids"] = json.loads(d.get("engineer_ids") or "[]")
        d["other_member_ids"] = json.loads(d.get("other_member_ids") or "[]")
        return d

    def upsert_group(self, g: dict[str, Any]) -> None:
        with self.transaction("upsert_group"):
            self._conn.execute(
                """INSERT INTO groups (
                       group_id, store_name, manager_ids, engineer_ids,
                       other_member_ids, engineering_leader_id, regional_manager_id,
                       ticket_seq, is_active)
                   VALUES (?,?,?,?,?,?,?, COALESCE((SELECT ticket_seq FROM groups
                       WHERE group_id=?), 0), 1)
                   ON CONFLICT(group_id) DO UPDATE SET
                       store_name=excluded.store_name,
                       manager_ids=excluded.manager_ids,
                       engineer_ids=excluded.engineer_ids,
                       other_member_ids=excluded.other_member_ids,
                       engineering_leader_id=excluded.engineering_leader_id,
                       regional_manager_id=excluded.regional_manager_id,
                       is_active=1""",
                (
                    g["group_id"], g["store_name"],
                    json.dumps(g.get("manager_ids", []), ensure_ascii=False),
                    json.dumps(g.get("engineer_ids", []), ensure_ascii=False),
                    json.dumps(g.get("other_member_ids", []), ensure_ascii=False),
                    g.get("engineering_leader_id", ""),
                    g.get("regional_manager_id", ""),
                    g["group_id"],
                ),
            )
        logger.info(
            "群配置写入 group_id=%s store=%s managers=%d engineers=%d",
            g["group_id"], g["store_name"],
            len(g.get("manager_ids", [])), len(g.get("engineer_ids", [])),
        )

    # ─────────────────────── 幂等 ───────────────────────
    def message_already_seen(self, message_id: str) -> bool:
        conn = self.connect()
        row = conn.execute(
            "SELECT result FROM processed_events WHERE message_id=?", (message_id,)
        ).fetchone()
        if row is not None:
            # 规范 3.2：幂等命中跳过（INFO）
            logger.info("幂等命中跳过 message_id=%s result=%s", message_id, row["result"])
            return True
        return False

    def record_processed_event(self, message_id: str, group_id: str, result: str) -> None:
        """必须在业务事务内调用；result: ARCHIVED/IGNORED/REJECTED 等。"""
        self._conn.execute(
            "INSERT OR IGNORE INTO processed_events (message_id, group_id, received_at, result)"
            " VALUES (?,?,?,?)",
            (message_id, group_id, _now_str(), result),
        )
