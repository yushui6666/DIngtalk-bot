"""收件箱工作器（计划书 §8.1、Task 6）。

单进程轮询：取 RECEIVED / 已到期 RETRY_PENDING → pipeline 处理 → 收件箱终态。
启动时把崩溃残留的 PROCESSING 重置回 RECEIVED。
"""

from __future__ import annotations

import asyncio
from typing import Any

from db import Database
from logger import get_logger

logger = get_logger(__name__)


class InboxWorker:
    def __init__(
        self,
        *,
        db: Database,
        pipeline: Any,
        notifier: Any,
        poll_interval: float = 0.5,
        batch: int = 10,
    ) -> None:
        self._db = db
        self._pipeline = pipeline
        self._notifier = notifier
        self._poll_interval = poll_interval
        self._batch = batch

    async def run(self) -> None:
        self._db.inbox_reset_stale()
        logger.info("收件箱工作器启动 poll_interval=%ss", self._poll_interval)
        while True:
            try:
                items = self._db.inbox_next_due(limit=self._batch)
                for item in items:
                    await self._pipeline.process(item)
                self._notifier.flush()
                if not items:
                    await asyncio.sleep(self._poll_interval)
            except asyncio.CancelledError:
                logger.info("收件箱工作器停止")
                raise
            except Exception as exc:
                logger.error("收件箱工作器循环异常 err=%s", exc)
                await asyncio.sleep(self._poll_interval)
