"""调度器测试：待确认过期 + SLA 时效提醒（去重）。"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pytest

from db import Database
from notifier import Notifier
from workers.scheduler import SchedulerWorker

GROUP = {"group_id": "G1", "store_name": "测试店",
         "manager_ids": ["mgr"], "engineer_ids": ["eng"], "other_member_ids": ["staff"]}

NOW = datetime(2026, 8, 12, 12, 0, 0)


@pytest.fixture()
def env(tmp_path, monkeypatch):
    db = Database(tmp_path / "sched.db")
    db.init_schema()
    db.upsert_group(GROUP)
    monkeypatch.setattr("config.ORDER_STORE_TABLE_PATH", tmp_path / "空.xlsx")  # 隔离共享表
    sent: list[str] = []
    notifier = Notifier(db, lambda target, text: sent.append(text))
    worker = SchedulerWorker(db=db, notifier=notifier, interval=60, remind_before_hours=6)
    yield SimpleNamespace(db=db, sent=sent, worker=worker, notifier=notifier)
    db.close()


class SimpleNamespace:
    def __init__(self, **kw):
        self.__dict__.update(kw)


def _insert_ticket(db: Database, ticket_no: str, deadline: str, status: str = "ACTIVE") -> int:
    with db.transaction("test_insert"):
        return db.insert_ticket({
            "ticket_no": ticket_no, "group_id": "G1", "store_name": "测试店",
            "reporter_id": "staff", "subject": "门", "location": "大厅",
            "problem_description": "坏了", "sla_days": 1,
            "initial_deadline_at": deadline, "current_deadline_at": deadline,
            "status": status,
        })


def test_pending_expiry_resolves(env):
    _insert_ticket(env.db, "T1", "2026-08-13 10:00:00")
    # 直接插入一条过期 WAITING 待确认
    with env.db.transaction("seed_pending"):
        env.db._conn.execute(
            """INSERT INTO pending_actions (source_message_id, group_id, user_id, intent,
                   candidate_ticket_ids_json, fields_json, expected_versions_json,
                   status, version, created_at, expires_at)
               VALUES ('pm', 'G1', 'mgr', 'ticket.complete', '[]', '{}', '{}',
                       'WAITING', 0, ?, ?)""",
            (NOW.strftime("%Y-%m-%d %H:%M:%S"), "2026-08-01 00:00:00"),
        )
    env.worker.scan_pending_expiry(NOW)
    row = env.db.connect().execute(
        "SELECT status FROM pending_actions WHERE source_message_id='pm'").fetchone()
    assert row["status"] == "EXPIRED"


def test_sla_reminder_before_deadline(env):
    _insert_ticket(env.db, "T1", "2026-08-12 15:00:00")  # 3h 后到期，在 6h 窗口内
    _insert_ticket(env.db, "T2", "2026-08-20 10:00:00")  # 远期，不提醒
    sent = env.worker.scan_sla_reminders(NOW)
    assert sent == 1
    assert any("即将到期" in s and "T1" in s for s in env.sent)
    assert not any("T2" in s for s in env.sent)


def test_sla_reminder_overdue(env):
    _insert_ticket(env.db, "T1", "2026-08-11 10:00:00")  # 已超时
    sent = env.worker.scan_sla_reminders(NOW)
    assert sent == 1
    assert any("已超时效" in s and "T1" in s for s in env.sent)


def test_sla_reminder_dedup(env):
    _insert_ticket(env.db, "T1", "2026-08-12 15:00:00")
    env.worker.scan_sla_reminders(NOW)
    env.worker.scan_sla_reminders(NOW)  # 第二次扫描不应重复提醒
    remind_count = sum(1 for s in env.sent if "即将到期" in s)
    assert remind_count == 1
    # outbox 里去重键唯一
    rows = env.db.connect().execute(
        "SELECT dedupe_key FROM notification_deliveries WHERE notification_type='sla_remind'"
    ).fetchall()
    assert len(rows) == 1


def test_completed_ticket_not_reminded(env):
    _insert_ticket(env.db, "T1", "2026-08-12 14:00:00", status="COMPLETED")
    sent = env.worker.scan_sla_reminders(NOW)
    assert sent == 0
