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
    last_payload: dict[str, Any] | None = None
    last_schema: dict[str, Any] | None = None
    last_idempotency_key: str | None = None

    async def complete_json(self, *, payload, schema, idempotency_key) -> dict[str, Any]:
        self.call_count += 1
        self.last_payload = payload
        self.last_schema = schema
        self.last_idempotency_key = idempotency_key
        if self.should_raise:
            raise self.should_raise
        if self.should_timeout:
            raise ModelTimeoutError("模拟超时")
        if self.response is None:
            raise ModelResponseError("模拟空响应")
        return self.response


class _FakeHttpResponse:
    status_code = 200
    text = ""

    def json(self) -> dict[str, Any]:
        return {
            "choices": [
                {
                    "message": {
                        "content": '{"intent":"chat.ignore","confidence":1,"fields":{}}'
                    }
                }
            ]
        }


class _CapturingAsyncClient:
    instances: list["_CapturingAsyncClient"] = []

    def __init__(self, *, timeout: float) -> None:
        self.timeout = timeout
        self.posts: list[dict[str, Any]] = []
        self.instances.append(self)

    async def __aenter__(self) -> "_CapturingAsyncClient":
        return self

    async def __aexit__(self, exc_type, exc, traceback) -> None:
        return None

    async def post(self, url: str, *, headers: dict[str, str], json: dict[str, Any]) -> _FakeHttpResponse:
        self.posts.append({"url": url, "headers": headers, "json": json})
        return _FakeHttpResponse()


class _ErrorHttpResponse:
    status_code = 400

    def __init__(self, text: str) -> None:
        self.text = text


class _ErrorAsyncClient(_CapturingAsyncClient):
    error_text = ""

    async def post(self, url: str, *, headers: dict[str, str], json: dict[str, Any]) -> _ErrorHttpResponse:
        self.posts.append({"url": url, "headers": headers, "json": json})
        return _ErrorHttpResponse(self.error_text)


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
async def test_classifier_filters_fields_not_allowed_for_intent():
    """协议中存在但不属于当前动作的字段也必须过滤。"""
    from semantics.classifier import SemanticClassifier

    protocol = _load_protocol()
    fake = FakeModelClient(response={
        "intent": "ticket.complete",
        "confidence": 0.9,
        "fields": {"cancel_reason": "偷偷取消", "completion_note": "已修复"},
    })
    classifier = SemanticClassifier(client=fake, protocol=protocol)
    result = await classifier.classify(_make_message(), candidates=[])
    assert "cancel_reason" not in result.fields
    assert result.fields["completion_note"] == "已修复"


@pytest.mark.asyncio
async def test_classifier_rejects_non_object_response():
    """模型客户端即使返回非对象 JSON，classifier 也应安全降级。"""
    from semantics.classifier import SemanticClassifier

    protocol = _load_protocol()
    fake = FakeModelClient(response=["not", "an", "object"])  # type: ignore[arg-type]
    classifier = SemanticClassifier(client=fake, protocol=protocol)
    result = await classifier.classify(_make_message(), candidates=[])
    assert result.intent == "chat.ignore"
    assert "response_error" in result.evidence[0]


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
async def test_classifier_rejects_target_ticket_outside_candidates():
    """模型不能选择当前候选集合外的目标工单。"""
    from semantics.classifier import SemanticClassifier
    from semantics.types import TicketCandidate

    protocol = _load_protocol()
    candidates = [
        TicketCandidate(
            ticket_id=1, ticket_no="T001", group_id="g-test",
            subject="门", location="大厅", problem_summary="下沉",
            status="ACTIVE", version=3,
        )
    ]
    fake = FakeModelClient(response={
        "intent": "ticket.complete",
        "confidence": 0.95,
        "fields": {},
        "ticket_no": "OTHER-GROUP-9",
    })
    classifier = SemanticClassifier(client=fake, protocol=protocol)
    result = await classifier.classify(_make_message(), candidates=candidates)
    assert result.target_ticket_no is None
    assert "ticket_no" in result.missing_fields


@pytest.mark.asyncio
async def test_classifier_payload_contains_protocol_candidates_and_pending_context():
    """模型输入包含受限协议、候选快照和待确认动作上下文。"""
    from semantics.classifier import SemanticClassifier
    from semantics.types import PendingAction, PendingActionStatus, TicketCandidate

    protocol = _load_protocol()
    fake = FakeModelClient(response={
        "intent": "chat.ignore", "confidence": 1.0, "fields": {}
    })
    candidates = [
        TicketCandidate(
            ticket_id=1, ticket_no="T001", group_id="g-test",
            subject="门", location="大厅", problem_summary="门体下沉",
            status="ACTIVE", version=3,
        )
    ]
    pending = PendingAction(
        id=9,
        source_message_id="source-1",
        group_id="g-test",
        user_id="u-test",
        intent="ticket.cancel",
        candidate_ticket_ids=(1,),
        fields={"cancel_reason": "误报"},
        expected_ticket_versions={1: 3},
        status=PendingActionStatus.WAITING,
        version=2,
        expires_at=datetime.now(),
    )
    classifier = SemanticClassifier(client=fake, protocol=protocol)
    await classifier.classify(_make_message(sender_role="MANAGER"), candidates, pending)

    payload = fake.last_payload
    system_prompt = payload["messages"][0]["content"]
    assert "发送人角色=MANAGER" in system_prompt
    assert "target_policy=MUST_EXIST" in system_prompt
    assert "confirmation_policy=" in system_prompt
    assert "T001" in system_prompt
    assert "门体下沉" in system_prompt
    assert "version=3" in system_prompt
    assert "pending_id=9" in system_prompt
    assert "cancel_reason" in system_prompt


@pytest.mark.asyncio
async def test_classifier_forces_clarify_for_multiple_business_actions():
    """同一消息明确包含两个业务动作时不得只选择其中一个。"""
    from semantics.classifier import SemanticClassifier

    protocol = _load_protocol()
    fake = FakeModelClient(response={
        "intent": "ticket.create",
        "confidence": 0.95,
        "fields": {
            "subject": "门",
            "location": "大厅",
            "problem_description": "坏了",
            "sla": "3天",
        },
    })
    classifier = SemanticClassifier(client=fake, protocol=protocol)
    result = await classifier.classify(
        _make_message(content="门坏了，位置在大厅，3天时效，先报修，修好之后就直接完毕"),
        candidates=[],
    )
    assert result.intent == "system.clarify"
    assert result.fields["clarification_reason"]


@pytest.mark.asyncio
async def test_classifier_allows_natural_language_ticket_select():
    """协议允许模型识别多候选中的明确工单选择。"""
    from semantics.classifier import SemanticClassifier
    from semantics.types import TicketCandidate

    protocol = _load_protocol()
    fake = FakeModelClient(response={
        "intent": "ticket.select",
        "confidence": 0.95,
        "fields": {"ticket_no": "T002"},
    })
    candidates = [
        TicketCandidate(1, "T001", "g-test", "门", "大厅", "下沉", "ACTIVE", 1),
        TicketCandidate(2, "T002", "g-test", "空调", "后厨", "漏水", "ACTIVE", 1),
    ]
    classifier = SemanticClassifier(client=fake, protocol=protocol)
    result = await classifier.classify(
        _make_message(content="我选第二张工单 T002"), candidates=candidates
    )
    assert result.intent == "ticket.select"
    assert result.target_ticket_no == "T002"


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


def test_model_client_default_timeout_is_60_seconds(monkeypatch):
    """Task 4 默认模型超时为 60 秒。"""
    from semantics.model_client import OpenAICompatibleModelClient

    monkeypatch.delenv("LLM_TIMEOUT_SECONDS", raising=False)
    client = OpenAICompatibleModelClient(api_key="sk-test")
    assert client.timeout_seconds == 60.0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("base_url", "response_format", "expected_type"),
    [
        ("https://api.openai.com/v1", "json_schema", "json_schema"),
        ("https://example.test/v1", "json_object", "json_object"),
        ("https://api.openai.com/v1", "auto", "json_schema"),
        ("https://example.test/v1", "auto", "json_object"),
    ],
)
async def test_model_client_response_format_modes(
    monkeypatch,
    base_url: str,
    response_format: str,
    expected_type: str,
):
    """响应格式由配置决定，且每次 complete_json 只发送一次请求。"""
    import httpx
    from semantics.model_client import OpenAICompatibleModelClient

    _CapturingAsyncClient.instances.clear()
    monkeypatch.setattr(httpx, "AsyncClient", _CapturingAsyncClient)
    client = OpenAICompatibleModelClient(
        base_url=base_url,
        api_key="sk-sensitive-test",
        model="test-model",
        response_format=response_format,
    )
    await client.complete_json(
        payload={"messages": [{"role": "system", "content": "test"}]},
        schema={"type": "object"},
        idempotency_key="msg-1",
    )

    assert len(_CapturingAsyncClient.instances) == 1
    instance = _CapturingAsyncClient.instances[0]
    assert instance.timeout == 60.0
    assert len(instance.posts) == 1
    request_format = instance.posts[0]["json"]["response_format"]
    assert request_format["type"] == expected_type
    if expected_type == "json_schema":
        assert request_format["json_schema"]["strict"] is True
    else:
        assert "json_schema" not in request_format


def test_model_client_rejects_unknown_response_format():
    """未知响应格式配置应在启动时失败。"""
    from semantics.model_client import OpenAICompatibleModelClient

    with pytest.raises(ValueError, match="LLM_RESPONSE_FORMAT"):
        OpenAICompatibleModelClient(api_key="sk-test", response_format="xml")


def test_extract_json_rejects_non_object():
    """JSON 数组等非对象响应不符合模型契约。"""
    from semantics.model_client import _extract_json

    with pytest.raises(ModelResponseError, match="JSON 对象"):
        _extract_json('["not", "object"]')


@pytest.mark.asyncio
async def test_model_client_redacts_api_key_from_http_error_log(monkeypatch, caplog):
    """兼容服务回显请求凭据时，日志不得泄露 API Key。"""
    import httpx
    from semantics.model_client import OpenAICompatibleModelClient

    secret = "sk-sensitive-do-not-log"
    _ErrorAsyncClient.instances.clear()
    _ErrorAsyncClient.error_text = f"invalid authorization Bearer {secret}"
    monkeypatch.setattr(httpx, "AsyncClient", _ErrorAsyncClient)
    client = OpenAICompatibleModelClient(
        base_url="https://example.test/v1",
        api_key=secret,
        response_format="json_object",
    )
    with pytest.raises(ModelResponseError):
        await client.complete_json(
            payload={"messages": [{"role": "system", "content": "test"}]},
            schema={"type": "object"},
            idempotency_key="msg-1",
        )
    assert secret not in caplog.text
