"""建单建议顾问（方案 v2.0 任务 5）。

定位：建单事务提交后的事件驱动增值动作——检索相似历史案例，
组装参考建议发群（群公开+提示工程师），并落 ticket_suggestions 台账。

设计约束（对齐项目哲学）：
- 建议是通知，不是工单动作：不进协议/权限/确认状态机，走 Outbox；
- 确定性模板组装，不调生成模型（零幻觉；LLM 润色留二期增强）；
- 检索弱（低于阈值）→ 沉默：宁可不建议，不错误引导；
- 任何异常 → 静默降级，工单主链路零影响；
- 已升级（ai_escalated）或已有建议的工单不再建议。
"""

from __future__ import annotations

from typing import Any, Optional

from logger import get_logger

logger = get_logger(__name__)

_SUGGESTION_TEMPLATE = (
    "💡 工单 {ticket_no} 相似案例参考（{count} 例）\n"
    "【故障现象】{problem}\n"
    "{cause_lines}"
    "{repair_lines}"
    "【参考工单】{refs}\n"
    "按建议恢复请回复「解决了」；仍异常请回复「未解决」，将请工程师处理\n"
    "—— 基于历史工单，仅供参考"
)


class TicketAdvisor:
    """建单后的相似案例建议器。"""

    def __init__(
        self,
        db: Any,
        store: Any,
        *,
        retriever: Any = None,
        embedding_client: Any = None,
        enabled: bool = True,
        top_k: int = 3,
    ) -> None:
        self._db = db
        self._store = store
        self._retriever = retriever
        self._embedding_client = embedding_client
        self.enabled = enabled
        self.top_k = top_k

    # ─────────────────────── 主入口 ───────────────────────

    def advise_for_new_ticket(self, ticket: dict[str, Any]) -> Optional[dict[str, Any]]:
        """为新建工单生成建议；无可靠依据返回 None（调用方保持沉默）。

        Returns:
            {"suggestion_id", "text", "doc_ids", "top_score"} 或 None
        """
        if not self.enabled:
            return None
        try:
            return self._advise(ticket)
        except Exception as exc:  # noqa: BLE001 —— 建议链路绝不影响工单主链路
            logger.warning(
                "建议生成失败（静默降级）ticket=%s: %s",
                ticket.get("ticket_no"), type(exc).__name__,
            )
            return None

    def _advise(self, ticket: dict[str, Any]) -> Optional[dict[str, Any]]:
        retriever = self._retriever or self._build_retriever()
        query = " ".join(filter(None, [
            str(ticket.get("subject") or ""),
            str(ticket.get("location") or ""),
            str(ticket.get("problem_description") or ""),
        ]))
        query_vector = self._embed_query(query)
        results = retriever.retrieve(query, query_vector=query_vector, limit=self.top_k)
        if not results:
            logger.info("无相似案例（低于阈值），保持沉默 ticket=%s", ticket.get("ticket_no"))
            return None

        text = self._format_suggestion(ticket, results)
        causes, repairs = self._extract_causes_repairs(results)
        suggestion_id = self._db.record_suggestion(
            ticket_id=ticket["id"],
            doc_ids=[r.doc_id for r in results],
            top_score=max(r.vector_score for r in results) if results else 0.0,
            content=text,
            detail={"causes": causes, "repairs": repairs},
        )
        logger.info(
            "建议已生成 ticket=%s docs=%s top_cos=%.3f",
            ticket.get("ticket_no"), [r.doc_id for r in results],
            max((r.vector_score for r in results), default=0.0),
        )
        return {
            "suggestion_id": suggestion_id,
            "text": text,
            "doc_ids": [r.doc_id for r in results],
            "top_score": max((r.vector_score for r in results), default=0.0),
        }

    def _build_retriever(self) -> Any:
        from qa.retriever import HybridRetriever
        return HybridRetriever(self._store)

    def _embed_query(self, query: str) -> Any:
        client = self._embedding_client
        if client is not None and not getattr(client, "is_configured", False):
            return None
        from qa.retriever import embed_with_client
        return embed_with_client(query, client)

    # ─────────────────────── 文案组装（确定性模板） ───────────────────────

    @staticmethod
    def _extract_causes_repairs(results: list[Any]) -> tuple[list[str], list[str]]:
        """从检索结果元数据提取（去重的）可能原因与历史处理。"""
        causes: list[str] = []
        repairs: list[str] = []
        for r in results:
            meta = getattr(r, "metadata", {}) or {}
            diag = meta.get("diagnosis")
            if diag and diag not in causes:
                causes.append(str(diag))
            repair = meta.get("repair_method")
            if repair and repair not in repairs:
                repairs.append(str(repair))
        return causes, repairs

    @classmethod
    def _format_suggestion(cls, ticket: dict[str, Any], results: list[Any]) -> str:
        causes: list[str] = []
        repairs: list[str] = cls._extract_causes_repairs(results)[1]
        for r in results:
            meta = getattr(r, "metadata", {}) or {}
            no = meta.get("ticket_no") or r.doc_id.split(":", 1)[-1]
            diag = meta.get("diagnosis")
            if diag:
                causes.append(f"{diag}（{no}）")
        refs = "、".join(
            (r.metadata or {}).get("ticket_no") or r.doc_id.split(":", 1)[-1]
            for r in results
        )
        cause_lines = f"【可能原因】{'；'.join(causes)}\n" if causes else ""
        repair_lines = f"【历史处理】{'／'.join(repairs)}\n" if repairs else ""
        return _SUGGESTION_TEMPLATE.format(
            ticket_no=ticket.get("ticket_no", ""),
            count=len(results),
            problem=str(ticket.get("problem_description") or "")[:60],
            cause_lines=cause_lines,
            repair_lines=repair_lines,
            refs=refs,
        )
