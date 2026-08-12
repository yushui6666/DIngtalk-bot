"""云端模型契约测试（计划书 Task 4）。

TDD 驱动：fake_client 模拟模型响应，验证 classifier 的边界行为。

覆盖场景（计划书步骤 1）：
- 超时
- 非 JSON 响应
- 协议外动作（提示注入）
- 字段幻觉
- 非法枚举值
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest

from semantics.model_client import ModelTimeoutError, ModelResponseError


# ───────────────────────── fake client ─────────────────────────


@dataclass
class FakeModelClient:
    """可编程的模型客户端桩，用于契约测试。

    通过设置 response / should_timeout / should_raise 控制行为。
    模拟 model_client.py 中各种异常路径。
    """
    response: dict[str, Any] | None = None
    should_timeout: bool = False
    should_raise: Exception | None = None
    call_count: int = 0

    async def complete_json(self, *, payload, schema, idempotency_key) -> dict[str, Any]:
        self.call_count += 1
        if self.should_raise:
            raise self.should_raise
        if self.should_timeout:
            raise ModelTimeoutError("模拟超时")
        if self.response is None:
            raise ModelResponseError("模拟空响应")
        return self.response


# ───────────────────────── 辅助 ─────────────────────────


def _load_protocol() -> Any:
    p = Path(__file__).resolve().parent.parent / "protocols" / "ticket_semantics.v4.json"
    from semantics.protocol_loader import load_protocol
    return load_protocol(p)


def _make_message(
    sender_role: str = "MANAGER",
    content: str = "门坏了需要修",
    message_id: str = "msg-test",
) -> Any:
    from models import NormalizedMessage
    return NormalizedMessage(
        message_id=message_id,
        group_id="g-test",
        sender_id="u-test",
        sender_name="测试用户",
        content=content,
        sent_at=datetime.now(),
        sender_role=sender_role,
    )


# ───────────────────────── 契约测试 ─────────────────────────


@pytest.mark.asyncio
async def test_classifier_rejects_unknown_intent():
    """模型返回协议外意图 → 降级为 chat.ignore（提示注入防护）。"""
    from semantics.classifier import SemanticClassifier

    protocol = _load_protocol()
    fake = FakeModelClient(response={"intent": "ticket.delete_all", "confidence": 0.9})
    classifier = SemanticClassifier(client=fake, protocol=protocol)
    msg = _make_message()
    result = await classifier.classify(msg, candidates=[])
    assert result.intent == "chat.ignore"
    assert result.source == "SEMANTIC_MODEL"


@pytest.mark.asyncio
async def test_classifier_handles_timeout():
    """模型超时 → 降级为 chat.ignore（不阻塞流程）。"""
    from semantics.classifier import SemanticClassifier

    protocol = _load_protocol()
    fake = FakeModelClient(should_timeout=True)
    classifier = SemanticClassifier(client=fake, protocol=protocol)
    msg = _make_message()
    result = await classifier.classify(msg, candidates=[])
    assert result.intent == "chat.ignore"
    assert result.source == "SEMANTIC_MODEL"
    assert fake.call_count == 1  # 单次调用，不内部重试


@pytest.mark.asyncio
async def test_classifier_handles_response_error():
    """模型返回非 JSON / HTTP 错误 → 降级。"""
    from semantics.classifier import SemanticClassifier

    protocol = _load_protocol()
    fake = FakeModelClient(should_raise=ModelResponseError("非 JSON 响应"))
    classifier = SemanticClassifier(client=fake, protocol=protocol)
    msg = _make_message()
    result = await classifier.classify(msg, candidates=[])
    assert result.intent == "chat.ignore"
    assert result.source == "SEMANTIC_MODEL"


@pytest.mark.asyncio
async def test_classifier_handles_network_error():
    """网络异常（OSError 等）→ 降级。"""
    from semantics.classifier import SemanticClassifier

    protocol = _load_protocol()
    fake = FakeModelClient(should_raise=OSError("连接被拒绝"))
    classifier = SemanticClassifier(client=fake, protocol=protocol)
    msg = _make_message()
    result = await classifier.classify(msg, candidates=[])
    assert result.intent == "chat.ignore"
    assert result.source == "SEMANTIC_MODEL"


@pytest.mark.asyncio
async def test_classifier_handles_empty_response():
    """模型返回空 dict → intent="" 不在协议中 → 降级。"""
    from semantics.classifier import SemanticClassifier

    protocol = _load_protocol()
    fake = FakeModelClient(response={})
    classifier = SemanticClassifier(client=fake, protocol=protocol)
    msg = _make_message()
    result = await classifier.classify(msg, candidates=[])
    assert result.intent == "chat.ignore"


@pytest.mark.asyncio
async def test_classifier_accepts_valid_create():
    """模型正确返回 ticket.create → 有效 SemanticDecision。"""
    from semantics.classifier import SemanticClassifier

    protocol = _load_protocol()
    fake = FakeModelClient(response={
        "intent": "ticket.create",
        "confidence": 0.92,
        "fields": {
            "subject": "门",
            "location": "大厅",
            "problem_description": "下沉",
            "sla": "3天",
        },
        "evidence": ["门坏了需要修"],
    })
    classifier = SemanticClassifier(client=fake, protocol=protocol)
    msg = _make_message()
    result = await classifier.classify(msg, candidates=[])
    assert result.intent == "ticket.create"
    assert result.source == "SEMANTIC_MODEL"
    assert result.intent_confidence == pytest.approx(0.92)
    assert result.fields.get("subject") == "门"
    assert result.fields.get("sla") == "3天"
    assert "门坏了需要修" in result.evidence


@pytest.mark.asyncio
async def test_classifier_skips_when_keyword_matched():
    """消息含显式关键词 → 模型直接跳过（keyword 快路径优先）。"""
    from semantics.classifier import SemanticClassifier

    protocol = _load_protocol()
    fake = FakeModelClient(response={"intent": "ticket.create", "confidence": 0.5})
    classifier = SemanticClassifier(client=fake, protocol=protocol)
    msg = _make_message(content="#报修 主题：门 位置：大厅 问题描述：坏了 时效：3天")
    result = await classifier.classify(msg, candidates=[])
    assert result.intent == "chat.ignore"
    assert result.source == "SEMANTIC_MODEL"
    assert fake.call_count == 0  # 关键词命中 → 模型零调用


@pytest.mark.asyncio
async def test_classifier_rejects_hallucinated_fields():
    """模型编造字段 → classifier 过滤掉不在字段词典中的键。"""
    from semantics.classifier import SemanticClassifier

    protocol = _load_protocol()
    fake = FakeModelClient(response={
        "intent": "ticket.create",
        "confidence": 0.9,
        "fields": {
            "subject": "门",
            "location": "大厅",
            "problem_description": "坏了",
            "sla": "3天",
            "budget": "100万",    # 幻觉字段
            "assignee": "超人",   # 幻觉字段
            "delete_command": "rm -rf",  # 注入尝试
        },
    })
    classifier = SemanticClassifier(client=fake, protocol=protocol)
    msg = _make_message()
    result = await classifier.classify(msg, candidates=[])
    assert result.intent == "ticket.create"
    assert "budget" not in result.fields
    assert "assignee" not in result.fields
    assert "delete_command" not in result.fields
    assert result.fields.get("subject") == "门"


@pytest.mark.asyncio
async def test_classifier_strips_unsafe_sla():
    """模型返回非法 SLA 枚举 → 移入 missing_fields。"""
    from semantics.classifier import SemanticClassifier

    protocol = _load_protocol()
    fake = FakeModelClient(response={
        "intent": "ticket.create",
        "confidence": 0.9,
        "fields": {
            "subject": "门",
            "location": "大厅",
            "problem_description": "坏了",
            "sla": "999天",  # 非法枚举
        },
    })
    classifier = SemanticClassifier(client=fake, protocol=protocol)
    msg = _make_message()
    result = await classifier.classify(msg, candidates=[])
    assert "sla" in result.missing_fields
    assert "sla" not in result.fields


@pytest.mark.asyncio
async def test_classifier_strips_unsafe_repair_method():
    """模型返回非法维修方式 → 移入 missing_fields。"""
    from semantics.classifier import SemanticClassifier

    protocol = _load_protocol()
    fake = FakeModelClient(response={
        "intent": "ticket.repair_plan.submit",
        "confidence": 0.9,
        "fields": {
            "repair_method": "随便修修",  # 非法枚举
        },
    })
    classifier = SemanticClassifier(client=fake, protocol=protocol)
    msg = _make_message()
    result = await classifier.classify(msg, candidates=[])
    assert "repair_method" in result.missing_fields
    assert "repair_method" not in result.fields


@pytest.mark.asyncio
async def test_classifier_strips_unsafe_urgency():
    """模型返回非法紧急度 → 移入 missing_fields。"""
    from semantics.classifier import SemanticClassifier

    protocol = _load_protocol()
    fake = FakeModelClient(response={
        "intent": "ticket.create",
        "confidence": 0.9,
        "fields": {
            "subject": "门",
            "location": "大厅",
            "problem_description": "坏了",
            "sla": "3天",
            "urgency": "非常紧急",  # 非法枚举
        },
    })
    classifier = SemanticClassifier(client=fake, protocol=protocol)
    msg = _make_message()
    result = await classifier.classify(msg, candidates=[])
    assert "urgency" in result.missing_fields


@pytest.mark.asyncio
async def test_classifier_clamps_confidence():
    """模型返回越界置信度 → 裁剪到 [0, 1]。"""
    from semantics.classifier import SemanticClassifier

    protocol = _load_protocol()
    fake = FakeModelClient(response={
        "intent": "ticket.create",
        "confidence": 1.5,  # 越界
        "fields": {"subject": "门", "location": "大厅", "problem_description": "坏了", "sla": "3天"},
    })
    classifier = SemanticClassifier(client=fake, protocol=protocol)
    msg = _make_message()
    result = await classifier.classify(msg, candidates=[])
    assert result.intent_confidence == pytest.approx(1.0)


@pytest.mark.asyncio
async def test_classifier_filters_candidate_scores_outside_group():
    """模型返回群外工单评分 → 过滤掉（§6.2 安全约束）。"""
    from semantics.classifier import SemanticClassifier
    from semantics.types import TicketCandidate

    protocol = _load_protocol()
    candidates = [
        TicketCandidate(
            ticket_id=1, ticket_no="T001", group_id="g-test",
            subject="收银机", location="前台", problem_summary="坏了",
            status="ACTIVE", version=1,
        ),
        TicketCandidate(
            ticket_id=2, ticket_no="T002", group_id="g-test",
            subject="门", location="大厅", problem_summary="下沉",
            status="ACTIVE", version=1,
        ),
    ]
    fake = FakeModelClient(response={
        "intent": "ticket.add_detail",
        "confidence": 0.93,
        "fields": {"content": "补充信息"},
        "candidate_scores": [
            {"ticket_no": "T001", "score": 0.95},
            {"ticket_no": "T002", "score": 0.30},
            {"ticket_no": "HACKED-999", "score": 0.99},  # 群外工单
        ],
    })
    classifier = SemanticClassifier(client=fake, protocol=protocol)
    msg = _make_message()
    result = await classifier.classify(msg, candidates=candidates)
    # 群外工单被过滤
    scored_nos = {s.ticket_no for s in result.candidate_scores}
    assert "HACKED-999" not in scored_nos
    assert "T001" in scored_nos


@pytest.mark.asyncio
async def test_classifier_single_http_call():
    """确认 classifier 只做单次 HTTP 调用，不内部重试（§8.1）。"""
    from semantics.classifier import SemanticClassifier

    protocol = _load_protocol()
    fake = FakeModelClient(should_timeout=True)
    classifier = SemanticClassifier(client=fake, protocol=protocol)
    msg = _make_message()
    await classifier.classify(msg, candidates=[])
    assert fake.call_count == 1


@pytest.mark.asyncio
async def test_classifier_model_client_not_configured():
    """OpenAICompatibleModelClient 在无 API Key 时 is_configured=False。"""
    from semantics.model_client import OpenAICompatibleModelClient

    client = OpenAICompatibleModelClient(api_key="")
    assert not client.is_configured

    client2 = OpenAICompatibleModelClient(api_key="sk-test")
    assert client2.is_configured
