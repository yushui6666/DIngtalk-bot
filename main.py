"""系统运行入口（计划书 Task 11）。

启动：初始化数据库 → 构建管道 → 并发启动「监听 + 收件箱 Worker + 调度器」。
用法::

    # 影子模式（只记录语义决策，不执行）
    python main.py --mode SHADOW

    # 辅助模式（模型来源动作一律待确认）
    python main.py --mode ASSISTED

    # 生产模式（按协议确认策略执行，默认）
    python main.py --mode PRODUCTION --duration 3600
"""

from __future__ import annotations

import argparse
import asyncio
import os
import subprocess
import sys
from pathlib import Path

from config import (
    GROUPS,
    LLM_ENABLED,
    LLM_API_KEY,
    LOG_DIR,
    load_groups,
)
from logger import get_logger, setup_logging

logger = get_logger(__name__)

_PROJECT_ROOT = Path(__file__).resolve().parent


def _load_env_file() -> None:
    """简单加载项目 .env（不覆盖已有环境变量）。"""
    env_path = _PROJECT_ROOT / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, _, value = stripped.partition("=")
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key:
            os.environ.setdefault(key, value)


def _dws_sender(target_id: str, text: str) -> None:
    """通过 dws CLI 发群消息（同步，低吞吐场景够用）。"""
    cmd = ["dws", "chat", "message", "send", "--group", target_id, "--text", text]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if proc.returncode != 0:
            logger.warning("dws 发送失败 target=%s stderr=%s", target_id, proc.stderr[:200])
    except Exception as exc:
        logger.warning("dws 发送异常 target=%s err=%s", target_id, exc)


def _build_pipeline(mode: str):
    from db import Database
    from notifier import Notifier
    from pipeline import MessageProcessingPipeline, RuntimeMode
    from routing.pending_actions import PendingActionService
    from routing.ticket_contexts import TicketContextStore
    from routing.ticket_router import TicketRouter
    from semantics.protocol_loader import load_protocol
    from tickets.executor import TicketCommandExecutor
    from tickets.repository import TicketRepository

    db = Database()
    db.init_schema()

    protocol_path = _PROJECT_ROOT / "protocols" / "ticket_semantics.v4.json"
    protocol = load_protocol(protocol_path)

    repo = TicketRepository(db)
    router = TicketRouter()
    context = TicketContextStore(db)
    pending = PendingActionService(db)
    executor = TicketCommandExecutor(db, repo)
    notifier = Notifier(db, _dws_sender)

    classifier = None
    if LLM_ENABLED and LLM_API_KEY:
        from semantics.classifier import SemanticClassifier
        from semantics.model_client import OpenAICompatibleModelClient

        client = OpenAICompatibleModelClient()
        classifier = SemanticClassifier(client=client, protocol=protocol)
        logger.info("云端模型已启用 model=%s base=%s", client.model, client.base_url)
    else:
        logger.info("云端模型未启用，仅走关键词快路径")

    pipeline = MessageProcessingPipeline(
        db=db,
        repo=repo,
        protocol=protocol,
        router=router,
        context=context,
        pending=pending,
        executor=executor,
        notifier=notifier,
        classifier=classifier,
        mode=RuntimeMode(mode),
    )
    return db, pipeline, notifier


async def _scheduler(db) -> None:
    """周期任务：待确认动作过期。"""
    from routing.pending_actions import PendingActionService
    from datetime import datetime

    pending_service = PendingActionService(db)
    while True:
        try:
            await asyncio.sleep(60)
            expired = pending_service.expire_due(datetime.now())
            if expired:
                logger.info("待确认动作过期清理 count=%d", expired)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("调度器异常 err=%s", exc)


async def main(mode: str, duration: int | None, group_filter: str | None) -> None:
    from event_listener import run_listeners
    from workers.inbox_worker import InboxWorker

    db, pipeline, notifier = _build_pipeline(mode)

    async def inbox_handler(msg) -> None:
        """监听回调：快速入箱，不在回调内调用模型。"""
        enqueued = db.enqueue_message(msg)
        logger.info("消息入箱 %s enqueued=%s", msg.brief(), enqueued)

    groups = GROUPS
    if group_filter:
        groups = [g for g in GROUPS if g["store_name"] == group_filter]
    load_groups()

    worker = InboxWorker(db=db, pipeline=pipeline, notifier=notifier)

    tasks = [
        asyncio.create_task(run_listeners(groups, inbox_handler)),
        asyncio.create_task(worker.run()),
        asyncio.create_task(_scheduler(db)),
    ]
    logger.info("系统启动 mode=%s groups=%d", mode, len(groups))

    try:
        if duration:
            await asyncio.sleep(duration)
        else:
            await asyncio.sleep(3600 * 24)  # 默认长驻
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        db.close()
        logger.info("系统停止")


if __name__ == "__main__":
    _load_env_file()
    parser = argparse.ArgumentParser(description="钉钉报修工单系统")
    parser.add_argument("--mode", choices=["SHADOW", "ASSISTED", "PRODUCTION"],
                        default="PRODUCTION", help="运行模式")
    parser.add_argument("--duration", type=int, default=None, help="运行秒数（默认长驻）")
    parser.add_argument("--group", default=None, help="仅监听指定群名")
    args = parser.parse_args()

    setup_logging(level="INFO", log_dir=LOG_DIR)
    try:
        asyncio.run(main(args.mode, args.duration, args.group))
    except KeyboardInterrupt:
        sys.exit(0)
