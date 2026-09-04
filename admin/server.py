"""
管理后台 API 服务
直接操作 SQLite 数据库，带前端页面
运行: python3 dingtalk_script/admin/server.py  (--port 8899)
       或  python3 -m admin.server
"""
from __future__ import annotations
import json
import os
import re
import sqlite3
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any, Optional
from urllib.parse import parse_qs

# 路径
BASE_DIR = Path(__file__).resolve().parent.parent  # dingtalk_script/
DB_PATH = BASE_DIR / "data" / "tickets.db"
STATIC_DIR = Path(__file__).resolve().parent / "static"

ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "")  # 默认无密码，直接可用；设置后需登录
ADMIN_TOKEN = "admin-token-dingtalk-2024"  # 简单 token
AUTH_REQUIRED = bool(ADMIN_PASSWORD)

# 友好表名
TABLE_LABELS = {
    "tickets": "🎫 工单 tickets",
    "groups": "👥 群组 groups",
    "inbox_messages": "📥 收件箱 inbox_messages",
    "messages": "💬 归档消息 messages",
    "message_ticket_links": "🔗 消息归属 message_ticket_links",
    "message_attachments": "📎 附件 message_attachments",
    "diagnosis_versions": "🩺 故障判断 diagnosis_versions",
    "repair_method_versions": "🔧 维修方案 repair_method_versions",
    "order_monitor": "📦 订单监控 order_monitor",
    "taobao_orders": "🛒 淘宝订单 taobao_orders",
    "ticket_special_cases": "⏸️ 特殊暂停 ticket_special_cases",
    "timeout_cycles": "⏰ 超时周期 timeout_cycles",
    "responsibility_cycles": "🔄 责任周期 responsibility_cycles",
    "pending_actions": "⏳ 待确认 pending_actions",
    "action_executions": "⚡ 执行记录 action_executions",
    "semantic_decisions": "🧠 语义决策 semantic_decisions",
    "notification_deliveries": "🔔 通知 notification_deliveries",
    "delivery_confirmations": "📬 签收确认 delivery_confirmations",
    "ticket_contexts": "📍 用户上下文 ticket_contexts",
    "processed_events": "✅ 已处理 processed_events",
    "schema_migrations": "🗃️ 迁移记录 schema_migrations",
}

TABLE_GROUPS = [
    ("核心业务", ["tickets", "groups", "ticket_special_cases"]),
    ("消息链路", ["inbox_messages", "messages", "message_ticket_links", "message_attachments", "semantic_decisions"]),
    ("维修流程", ["diagnosis_versions", "repair_method_versions", "timeout_cycles", "responsibility_cycles"]),
    ("订单/物流", ["order_monitor", "taobao_orders", "delivery_confirmations"]),
    ("系统/队列", ["pending_actions", "action_executions", "ticket_contexts", "notification_deliveries", "processed_events", "schema_migrations"]),
]

# ============ 工具 ============
def get_conn(readonly: bool = False) -> sqlite3.Connection:
    uri = f"file:{DB_PATH}?mode={'ro' if readonly else 'rwc'}"
    conn = sqlite3.connect(uri, uri=True, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def list_tables() -> list[dict]:
    conn = get_conn(readonly=True)
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name").fetchall()
    result = []
    for r in rows:
        name = r[0]
        try:
            cnt = conn.execute(f'SELECT COUNT(*) FROM "{name}"').fetchone()[0]
        except Exception:
            cnt = 0
        result.append({"name": name, "label": TABLE_LABELS.get(name, name), "count": cnt})
    conn.close()
    return result

def get_schema(table: str) -> list[dict]:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", table):
        raise ValueError("非法表名")
    conn = get_conn(readonly=True)
    rows = conn.execute(f'PRAGMA table_info("{table}")').fetchall()
    conn.close()
    return [dict(r) for r in rows]

def get_primary_key(table: str) -> Optional[str]:
    schema = get_schema(table)
    pks = [c["name"] for c in schema if c["pk"]]
    if len(pks) == 1:
        return pks[0]
    return pks[0] if pks else None

def query_rows(table: str, page: int = 1, page_size: int = 20, search: str = "", order_by: str = "", order_dir: str = "DESC") -> dict:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", table):
        raise ValueError("非法表名")
    schema = get_schema(table)
    cols = [c["name"] for c in schema]
    if order_by and order_by not in cols:
        order_by = ""
    if order_dir not in ("ASC", "DESC"):
        order_dir = "DESC"
    # 默认排序：有 id 按 id 倒序，有 created_at 按 created_at
    if not order_by:
        for cand in ["id", "created_at", "sent_at", "received_at"]:
            if cand in cols:
                order_by = cand
                break
        if not order_by and cols:
            order_by = cols[0]

    conn = get_conn(readonly=True)
    where_sql = ""
    params: list[Any] = []
    if search:
        likes = " OR ".join([f'"{c}" LIKE ?' for c in cols])
        where_sql = f"WHERE ({likes})"
        params = [f"%{search}%" for _ in cols]

    total = conn.execute(f'SELECT COUNT(*) FROM "{table}" {where_sql}', params).fetchone()[0]
    offset = (page - 1) * page_size
    rows = conn.execute(
        f'SELECT * FROM "{table}" {where_sql} ORDER BY "{order_by}" {order_dir} LIMIT ? OFFSET ?',
        (*params, page_size, offset)
    ).fetchall()
    conn.close()
    # 尝试友好展示：groups 的 JSON 字段展开
    data = [dict(r) for r in rows]
    return {"total": total, "page": page, "pageSize": page_size, "totalPages": (total + page_size - 1)//page_size if page_size else 1, "rows": data, "columns": schema, "orderBy": order_by, "orderDir": order_dir}

# ============ FastAPI ============
try:
    from fastapi import FastAPI, Request, Response, HTTPException
    from fastapi.responses import HTMLResponse, JSONResponse, FileResponse
    from fastapi.staticfiles import StaticFiles
    import uvicorn
    HAS_FASTAPI = True
except ImportError:
    HAS_FASTAPI = False

if HAS_FASTAPI:
    app = FastAPI(title="报修工单管理后台", docs_url="/api/docs", redoc_url="/api/redoc")

    @app.get("/", response_class=HTMLResponse)
    def index():
        p = STATIC_DIR / "index.html"
        if p.exists():
            html = p.read_text(encoding="utf-8")
            # 禁止缓存，确保修复后立即生效
            return HTMLResponse(html, headers={"Cache-Control": "no-store, no-cache, must-revalidate", "Pragma": "no-cache"})
        return HTMLResponse("<h1>前端文件缺失</h1>")

    @app.post("/api/login")
    async def login(req: Request):
        if not AUTH_REQUIRED:
            resp = JSONResponse({"ok": True, "token": ADMIN_TOKEN})
            resp.set_cookie("admin_token", ADMIN_TOKEN, httponly=False, max_age=86400*7, path="/")
            return resp
        body = await req.json()
        pwd = body.get("password", "")
        if pwd == ADMIN_PASSWORD:
            resp = JSONResponse({"ok": True, "token": ADMIN_TOKEN})
            resp.set_cookie("admin_token", ADMIN_TOKEN, httponly=False, max_age=86400*7, path="/")
            return resp
        raise HTTPException(401, "密码错误")

    def check_auth(req: Request):
        if not AUTH_REQUIRED:
            return True
        token = req.headers.get("x-admin-token") or req.cookies.get("admin_token") or req.query_params.get("token")
        if token == ADMIN_TOKEN:
            return True
        return False

    @app.get("/api/me")
    def me(req: Request):
        if not AUTH_REQUIRED:
            return {"authed": True, "authRequired": False}
        if check_auth(req):
            return {"authed": True, "authRequired": True}
        return {"authed": False, "authRequired": True}

    @app.get("/api/config")
    def config_info():
        return {"authRequired": AUTH_REQUIRED, "dbPath": str(DB_PATH)}

    @app.get("/api/stats")
    def stats(req: Request):
        if not check_auth(req): raise HTTPException(401, "未登录")
        conn = get_conn(readonly=True)
        def cnt(sql, p=()): return conn.execute(sql, p).fetchone()[0]
        try:
            s = {
                "tickets_total": cnt("SELECT COUNT(*) FROM tickets"),
                "tickets_active": cnt("SELECT COUNT(*) FROM tickets WHERE status IN ('ACTIVE','ACTIVE_OVERDUE')"),
                "tickets_pending": cnt("SELECT COUNT(*) FROM tickets WHERE status='PENDING_CONFIRM'"),
                "tickets_completed": cnt("SELECT COUNT(*) FROM tickets WHERE status='COMPLETED'"),
                "tickets_cancelled": cnt("SELECT COUNT(*) FROM tickets WHERE status='CANCELLED'"),
                "tickets_stopped": cnt("SELECT COUNT(*) FROM tickets WHERE status='STOPPED'"),
                "tickets_negotiating": cnt("SELECT COUNT(*) FROM tickets WHERE status='PENDING_NEGOTIATION'"),
                "inbox_total": cnt("SELECT COUNT(*) FROM inbox_messages"),
                "inbox_pending": cnt("SELECT COUNT(*) FROM inbox_messages WHERE status IN ('RECEIVED','RETRY_PENDING','PROCESSING')"),
                "inbox_dead": cnt("SELECT COUNT(*) FROM inbox_messages WHERE status='DEAD_LETTER'"),
                "groups": cnt("SELECT COUNT(*) FROM groups"),
                "orders": cnt("SELECT COUNT(*) FROM order_monitor"),
                "attachments": cnt("SELECT COUNT(*) FROM message_attachments"),
                "db_size": DB_PATH.stat().st_size if DB_PATH.exists() else 0,
                "db_path": str(DB_PATH),
            }
            # 最近工单
            recent = [dict(r) for r in conn.execute("SELECT ticket_no, status, store_name, created_at FROM tickets ORDER BY id DESC LIMIT 5").fetchall()]
            s["recent_tickets"] = recent
        finally:
            conn.close()
        return s

    @app.get("/api/tables")
    def api_tables(req: Request):
        if not check_auth(req): raise HTTPException(401, "未登录")
        return {"tables": list_tables(), "groups": TABLE_GROUPS}

    @app.get("/api/table/{table}/schema")
    def api_schema(table: str, req: Request):
        if not check_auth(req): raise HTTPException(401, "未登录")
        return {"table": table, "schema": get_schema(table), "pk": get_primary_key(table)}

    @app.get("/api/table/{table}/rows")
    def api_rows(table: str, req: Request, page: int = 1, pageSize: int = 20, search: str = "", orderBy: str = "", orderDir: str = "DESC"):
        if not check_auth(req): raise HTTPException(401, "未登录")
        try:
            return query_rows(table, page, pageSize, search, orderBy, orderDir)
        except Exception as e:
            raise HTTPException(400, str(e))

    @app.post("/api/table/{table}/rows")
    async def api_insert(table: str, req: Request):
        if not check_auth(req): raise HTTPException(401, "未登录")
        body = await req.json()
        data = body.get("data") or body
        if not isinstance(data, dict) or not data:
            raise HTTPException(400, "data 不能为空")
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", table):
            raise HTTPException(400, "非法表名")
        # 备份
        schema = get_schema(table)
        cols = [c["name"] for c in schema]
        # 过滤非法列
        clean = {k: v for k, v in data.items() if k in cols}
        if not clean:
            raise HTTPException(400, "无有效列")
        # 值处理：空字符串转 None 对于可空字段？保留原值
        for k, v in list(clean.items()):
            if v == "":
                clean[k] = None
        placeholders = ",".join(["?"]*len(clean))
        col_sql = ",".join([f'"{k}"' for k in clean.keys()])
        conn = get_conn()
        try:
            cur = conn.execute(f'INSERT INTO "{table}" ({col_sql}) VALUES ({placeholders})', list(clean.values()))
            conn.commit()
            return {"ok": True, "last_rowid": cur.lastrowid}
        except Exception as e:
            raise HTTPException(400, f"插入失败: {e}")
        finally:
            conn.close()

    @app.put("/api/table/{table}/rows")
    async def api_update(table: str, req: Request):
        if not check_auth(req): raise HTTPException(401, "未登录")
        body = await req.json()
        pk = body.get("pk")
        pk_value = body.get("pkValue")
        data = body.get("data") or {}
        # 兼容前端：直接传 {pk_col: value, ...data}
        if not pk or pk_value is None:
            # 尝试自动推断
            schema = get_schema(table)
            pk_candidates = [c["name"] for c in schema if c["pk"]]
            if pk_candidates and pk_candidates[0] in body:
                pk = pk_candidates[0]
                pk_value = body[pk]
                data = {k: v for k, v in body.items() if k != pk}
            else:
                raise HTTPException(400, "需提供 pk / pkValue")
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", table) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", pk):
            raise HTTPException(400, "非法表名/主键")
        schema = get_schema(table)
        cols = [c["name"] for c in schema]
        if pk not in cols:
            raise HTTPException(400, "主键不存在")
        clean = {k: v for k, v in data.items() if k in cols and k != pk}
        if not clean:
            raise HTTPException(400, "无可更新列")
        for k, v in list(clean.items()):
            if v == "":
                clean[k] = None
        set_sql = ",".join([f'"{k}"=?' for k in clean.keys()])
        conn = get_conn()
        try:
            cur = conn.execute(f'UPDATE "{table}" SET {set_sql} WHERE "{pk}"=?', (*clean.values(), pk_value))
            conn.commit()
            if cur.rowcount == 0:
                raise HTTPException(404, "未找到行")
            return {"ok": True, "rowcount": cur.rowcount}
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(400, f"更新失败: {e}")
        finally:
            conn.close()

    @app.delete("/api/table/{table}/rows")
    async def api_delete(table: str, req: Request):
        if not check_auth(req): raise HTTPException(401, "未登录")
        body = await req.json() if await req.body() else {}
        pk = body.get("pk") or req.query_params.get("pk")
        pk_value = body.get("pkValue") or req.query_params.get("pkValue")
        if not pk or pk_value is None:
            raise HTTPException(400, "需提供 pk / pkValue")
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", table) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", pk):
            raise HTTPException(400, "非法")
        conn = get_conn()
        try:
            cur = conn.execute(f'DELETE FROM "{table}" WHERE "{pk}"=?', (pk_value,))
            conn.commit()
            return {"ok": True, "rowcount": cur.rowcount}
        except Exception as e:
            raise HTTPException(400, f"删除失败: {e}")
        finally:
            conn.close()

    @app.post("/api/sql")
    async def api_sql(req: Request):
        if not check_auth(req): raise HTTPException(401, "未登录")
        body = await req.json()
        sql = (body.get("sql") or "").strip()
        if not sql:
            raise HTTPException(400, "SQL 不能为空")
        # 安全提示：允许所有 SQL，但记录
        readonly = sql.strip().lower().startswith("select") or sql.strip().lower().startswith("pragma") or sql.strip().lower().startswith("explain")
        conn = get_conn(readonly=readonly)
        try:
            # 自动备份（写操作前）
            if not readonly:
                backup_path = DB_PATH.parent / f"tickets.db.bak_admin_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                try: shutil.copy2(DB_PATH, backup_path)
                except: pass
            cur = conn.execute(sql)
            if readonly or sql.strip().lower().startswith("select"):
                rows = cur.fetchall()
                cols = [d[0] for d in cur.description] if cur.description else []
                data = [dict(r) if isinstance(r, sqlite3.Row) else dict(zip(cols, r)) for r in rows]
                # 如果是 Row 但无描述，回退
                if rows and not cols:
                    data = [dict(r) for r in rows]
                return {"ok": True, "columns": cols, "rows": data[:500], "rowcount": len(data), "truncated": len(data) >= 500}
            else:
                conn.commit()
                return {"ok": True, "rowcount": cur.rowcount, "lastrowid": cur.lastrowid}
        except Exception as e:
            raise HTTPException(400, f"SQL 错误: {e}")
        finally:
            conn.close()

    @app.post("/api/backup")
    def api_backup(req: Request):
        if not check_auth(req): raise HTTPException(401, "未登录")
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        dst = DB_PATH.parent / f"tickets.db.bak_admin_{ts}"
        shutil.copy2(DB_PATH, dst)
        return {"ok": True, "path": str(dst)}

    # ── 工单快捷操作（傻瓜式新增/删除，走业务逻辑，保证编号与系统一致） ──
    SLA_LABELS = ["1天", "3天", "7天", "待商榷"]

    # 删除时级联清理的子表（ticket_id 直接关联）；不在此列的仅为审计/过期型数据：
    # pending_actions（JSON 引用，会过期）、action_executions（审计留痕）、
    # semantic_decisions（按 message_id 审计）、inbox_messages（独立收件箱）。
    _CASCADE_TABLES = [
        "messages", "diagnosis_versions", "repair_method_versions",
        "timeout_cycles", "responsibility_cycles", "message_ticket_links",
        "ticket_special_cases", "delivery_confirmations",
        "notification_deliveries", "order_monitor", "ticket_contexts",
    ]

    def _biz_db():
        """业务库 Database（复用 tickets.repository，保证建单逻辑与主系统一致）。"""
        import sys as _sys
        if str(BASE_DIR) not in _sys.path:
            _sys.path.insert(0, str(BASE_DIR))
        from db import Database
        return Database(str(DB_PATH))

    @app.get("/api/ticket-options")
    def api_ticket_options(req: Request):
        if not check_auth(req): raise HTTPException(401, "未登录")
        conn = get_conn(readonly=True)
        try:
            rows = conn.execute(
                "SELECT group_id, store_name, ticket_seq FROM groups ORDER BY store_name"
            ).fetchall()
            groups = [dict(r) for r in rows]
        finally:
            conn.close()
        return {"groups": groups, "slaLabels": SLA_LABELS}

    @app.post("/api/tickets/quick-create")
    async def api_ticket_quick_create(req: Request):
        if not check_auth(req): raise HTTPException(401, "未登录")
        body = await req.json()
        group_id = (body.get("group_id") or "").strip()
        subject = (body.get("subject") or "").strip()
        location = (body.get("location") or "").strip()
        problem = (body.get("problem_description") or "").strip()
        sla_label = (body.get("sla_label") or "3天").strip()
        if not group_id:
            raise HTTPException(400, "请选择门店")
        if not subject:
            raise HTTPException(400, "请填写主题")
        if not location:
            raise HTTPException(400, "请填写位置")
        if not problem:
            raise HTTPException(400, "请填写问题描述")
        if sla_label not in SLA_LABELS:
            raise HTTPException(400, f"时效必须是其中之一：{'/'.join(SLA_LABELS)}")
        db = _biz_db()
        from tickets.repository import TicketRepository
        with db.transaction("admin_quick_create"):
            group = db.get_group(group_id)
            if group is None:
                raise HTTPException(400, "门店不存在")
            repo = TicketRepository(db)
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            ticket_id = repo.create_ticket(
                group=group, reporter_id="admin-manual",
                subject=subject, location=location,
                problem_description=problem, sla_label=sla_label, now=now,
            )
            ticket = db.get_ticket(ticket_id)
        try:
            db.close()
        except Exception:
            pass
        return {"ok": True, "ticket": ticket}

    # 允许手动切换的目标状态（ACTIVE_OVERDUE 由扫描器判定，不开放手改）
    ALLOWED_STATUS = ["ACTIVE", "PENDING_CONFIRM", "PENDING_NEGOTIATION",
                      "COMPLETED", "CANCELLED", "STOPPED"]
    _TERMINAL = {"COMPLETED", "CANCELLED", "STOPPED"}

    @app.post("/api/tickets/{ticket_id}/status")
    async def api_ticket_set_status(ticket_id: int, req: Request):
        """手动改工单状态，连带副作用与主系统 executor 对齐：
        终态（完成/取消/停修）→ 写 closed_at 与对应留痕 + 关闭责任周期 + 清用户上下文；
        从终态切回进行中 → 按重开处理（closed_at 清空、reopen_count+1、清 SLA 去重）。
        """
        if not check_auth(req): raise HTTPException(401, "未登录")
        body = await req.json()
        status = (body.get("status") or "").strip()
        if status not in ALLOWED_STATUS:
            raise HTTPException(400, f"状态必须是其中之一：{'/'.join(ALLOWED_STATUS)}")
        db = _biz_db()
        with db.transaction("admin_set_status"):
            t = db.get_ticket(ticket_id)
            if t is None:
                raise HTTPException(404, "工单不存在")
            prev = t["status"]
            if status == prev:
                return {"ok": True, "unchanged": True, "ticket": t}
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            if status == "ACTIVE" and prev in _TERMINAL:
                db._conn.execute(
                    "UPDATE tickets SET status='ACTIVE', closed_at=NULL,"
                    " reopen_count=reopen_count+1, waiting_side='NONE', waiting_since=NULL,"
                    " current_responsibility_cycle_id=NULL, version=version+1 WHERE id=?",
                    (ticket_id,),
                )
                db.close_responsibility_cycles(ticket_id, "admin-manual")
                db.clear_ticket_sla_dedupe(ticket_id)
            elif status == "COMPLETED":
                db._conn.execute(
                    "UPDATE tickets SET status='COMPLETED', closed_at=?,"
                    " completed_confirm_by='admin-manual', completed_confirm_at=?,"
                    " version=version+1 WHERE id=?",
                    (now, now, ticket_id),
                )
                db.close_responsibility_cycles(ticket_id, "admin-manual")
                db._conn.execute("DELETE FROM ticket_contexts WHERE ticket_id=?", (ticket_id,))
            elif status == "CANCELLED":
                db._conn.execute(
                    "UPDATE tickets SET status='CANCELLED', closed_at=?, cancelled_at=?,"
                    " cancelled_by='admin-manual', cancel_reason='管理员后台手动取消',"
                    " version=version+1 WHERE id=?",
                    (now, now, ticket_id),
                )
                db.close_responsibility_cycles(ticket_id, "admin-manual")
                db._conn.execute("DELETE FROM ticket_contexts WHERE ticket_id=?", (ticket_id,))
            elif status == "STOPPED":
                db._conn.execute(
                    "UPDATE tickets SET status='STOPPED', closed_at=?, stopped_at=?,"
                    " stopped_by='admin-manual', stop_reason='管理员后台手动停修',"
                    " version=version+1 WHERE id=?",
                    (now, now, ticket_id),
                )
                db.close_responsibility_cycles(ticket_id, "admin-manual")
                db._conn.execute("DELETE FROM ticket_contexts WHERE ticket_id=?", (ticket_id,))
            else:
                # ACTIVE（非终态间）/ PENDING_CONFIRM / PENDING_NEGOTIATION：仅切状态
                db._conn.execute(
                    "UPDATE tickets SET status=?, version=version+1 WHERE id=?",
                    (status, ticket_id),
                )
            ticket = db.get_ticket(ticket_id)
        try:
            db.close()
        except Exception:
            pass
        return {"ok": True, "prev": prev, "ticket": ticket}

    @app.get("/api/tickets/{ticket_id}/dependents")
    def api_ticket_dependents(ticket_id: int, req: Request):
        if not check_auth(req): raise HTTPException(401, "未登录")
        conn = get_conn(readonly=True)
        try:
            t = conn.execute("SELECT * FROM tickets WHERE id=?", (ticket_id,)).fetchone()
            if t is None:
                raise HTTPException(404, "工单不存在")
            counts = {}
            for tbl in _CASCADE_TABLES:
                try:
                    counts[tbl] = conn.execute(
                        f'SELECT COUNT(*) FROM "{tbl}" WHERE ticket_id=?', (ticket_id,)
                    ).fetchone()[0]
                except Exception:
                    counts[tbl] = 0
            try:
                counts["message_attachments"] = conn.execute(
                    "SELECT COUNT(*) FROM message_attachments WHERE ticket_id=?", (ticket_id,)
                ).fetchone()[0]
            except Exception:
                counts["message_attachments"] = 0
            return {"ok": True, "ticket": dict(t), "counts": counts}
        finally:
            conn.close()

    @app.delete("/api/tickets/{ticket_id}")
    def api_ticket_delete(ticket_id: int, req: Request):
        if not check_auth(req): raise HTTPException(401, "未登录")
        conn = get_conn(readonly=True)
        try:
            t = conn.execute("SELECT * FROM tickets WHERE id=?", (ticket_id,)).fetchone()
            if t is None:
                raise HTTPException(404, "工单不存在")
            ticket_no = t["ticket_no"]
        finally:
            conn.close()
        # 先整库备份（与 SQL 控制台写操作一致）
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup = DB_PATH.parent / f"tickets.db.bak_admin_del_{ticket_id}_{ts}"
        shutil.copy2(DB_PATH, backup)
        db = _biz_db()
        with db.transaction("admin_delete_ticket"):
            deleted = {}
            for tbl in _CASCADE_TABLES:
                cur = db._conn.execute(
                    f'DELETE FROM "{tbl}" WHERE ticket_id=?', (ticket_id,)
                )
                deleted[tbl] = cur.rowcount
            # 附件保留文件记录，仅解除归属
            cur = db._conn.execute(
                "UPDATE message_attachments SET ticket_id=NULL WHERE ticket_id=?", (ticket_id,)
            )
            deleted["message_attachments_unlinked"] = cur.rowcount
            cur = db._conn.execute("DELETE FROM tickets WHERE id=?", (ticket_id,))
            if cur.rowcount == 0:
                raise HTTPException(404, "工单不存在")
        try:
            db.close()
        except Exception:
            pass
        return {"ok": True, "ticket_no": ticket_no, "deleted": deleted, "backup": str(backup)}

    # 静态文件（若有其他资源）
    try:
        app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
    except: pass

    def run(port: int = 8899):
        if AUTH_REQUIRED:
            print(f"✅ 管理后台启动 http://127.0.0.1:{port}  (DB: {DB_PATH}) 密码: {ADMIN_PASSWORD}")
        else:
            print(f"✅ 管理后台启动 http://127.0.0.1:{port}  (DB: {DB_PATH}) 无密码直接访问")
        uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")

    if __name__ == "__main__":
        import argparse
        ap = argparse.ArgumentParser()
        ap.add_argument("--port", type=int, default=8899)
        ap.add_argument("--host", type=str, default="0.0.0.0")
        args = ap.parse_args()
        uvicorn.run(app, host=args.host, port=args.port)

else:
    print("FastAPI 未安装，请 pip install fastapi uvicorn")
