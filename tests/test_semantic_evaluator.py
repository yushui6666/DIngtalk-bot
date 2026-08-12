"""离线评测器测试（计划书 Task 3）。

TDD 驱动：先写会失败的指标计算测试，再实现 evaluator.py。
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest


# ───────────────────────── 辅助 ─────────────────────────


def _load_dataset(name: str = "semantic_cases.json") -> list[dict[str, Any]]:
    import json
    p = Path(__file__).resolve().parent / "fixtures" / name
    return json.loads(p.read_text(encoding="utf-8"))


def _load_protocol() -> Any:
    p = Path(__file__).resolve().parent.parent / "protocols" / "ticket_semantics.v4.json"
    from semantics.protocol_loader import load_protocol
    return load_protocol(p)


# ───────────────────────── 失败测试 ─────────────────────────


def test_evaluator_counts_wrong_intent_as_error():
    """意图预测错误被正确统计。"""
    from semantics.evaluator import evaluate, LabeledCase, EvalPrediction

    cases = [LabeledCase(id="c1", text="#报修 主题：门", expected_intent="ticket.create")]
    preds = [EvalPrediction(id="c1", predicted_intent="ticket.complete", predicted_fields={})]
    report = evaluate(cases, preds)
    assert report.intent_accuracy < 1.0


def test_evaluator_counts_correct_intent():
    """意图预测正确得分 1.0。"""
    from semantics.evaluator import evaluate, LabeledCase, EvalPrediction

    cases = [LabeledCase(id="c1", text="#报修 主题：门", expected_intent="ticket.create")]
    preds = [EvalPrediction(id="c1", predicted_intent="ticket.create", predicted_fields={})]
    report = evaluate(cases, preds)
    assert report.intent_accuracy == 1.0


def test_evaluator_counts_field_accuracy():
    """字段抽取正确率被计算。"""
    from semantics.evaluator import evaluate, LabeledCase, EvalPrediction

    cases = [
        LabeledCase(id="c1", text="msg", expected_intent="ticket.create",
                     expected_fields={"subject": "门", "sla": "3天"}),
    ]
    preds = [
        EvalPrediction(id="c1", predicted_intent="ticket.create",
                       predicted_fields={"subject": "门", "sla": "1天"}),
    ]
    report = evaluate(cases, preds)
    # 主题正确，时效错误 → field_accuracy < 1.0
    assert 0.0 < report.field_accuracy < 1.0


def test_evaluator_handles_empty_dataset():
    """空数据集不崩溃。"""
    from semantics.evaluator import evaluate, LabeledCase, EvalPrediction
    report = evaluate([], [])
    assert report.intent_accuracy == 0.0
    assert report.field_accuracy == 0.0


def test_evaluator_ignores_id_mismatch():
    """ID 不匹配的预测被忽略（不参与计算）。"""
    from semantics.evaluator import evaluate, LabeledCase, EvalPrediction

    cases = [LabeledCase(id="c1", text="msg", expected_intent="ticket.create")]
    preds = [EvalPrediction(id="c2", predicted_intent="ticket.complete", predicted_fields={})]
    report = evaluate(cases, preds)
    # c1 没有对应预测，准确率为 0
    assert report.intent_accuracy == 0.0


# ───────────────────────── 数据集集成测试 ─────────────────────────


def test_dataset_all_cases_have_required_fields():
    """所有标注用例都有必填字段。"""
    cases = _load_dataset("semantic_cases.json")
    assert len(cases) >= 10, f"至少 10 个标注用例，当前 {len(cases)}"
    for c in cases:
        assert "id" in c, f"缺少 id: {c}"
        assert "text" in c, f"缺少 text: {c}"
        assert "expected_intent" in c, f"缺少 expected_intent: {c}"


def test_dataset_covers_all_intent_types():
    """数据集覆盖主要意图类型。"""
    cases = _load_dataset("semantic_cases.json")
    intents = {c["expected_intent"] for c in cases}
    required = {"ticket.create", "ticket.diagnosis.submit", "ticket.repair_plan.submit",
                "ticket.complete", "chat.ignore", "system.clarify"}
    missing = required - intents
    assert not missing, f"缺少意图类型: {missing}"


def test_blind_dataset_structure():
    """盲测集结构与训练集一致。"""
    blind = _load_dataset("semantic_cases.blind.json")
    assert len(blind) >= 3, f"至少 3 个盲测用例，当前 {len(blind)}"
    for c in blind:
        assert "id" in c
        assert "text" in c
        assert "expected_intent" in c


# ───────────────────────── 关键词匹配器评测 ─────────────────────────


def test_keyword_matcher_on_labeled_dataset():
    """关键词匹配器在标注数据集上的表现可测量。"""
    from semantics.keyword_matcher import match_keyword
    from semantics.evaluator import evaluate, LabeledCase, EvalPrediction

    protocol = _load_protocol()
    raw_cases = _load_dataset("semantic_cases.json")

    cases: list[LabeledCase] = []
    preds: list[EvalPrediction] = []

    for c in raw_cases:
        cases.append(LabeledCase(
            id=c["id"],
            text=c["text"],
            expected_intent=c["expected_intent"],
            expected_fields=c.get("expected_fields", {}),
        ))
        result = match_keyword(c["text"], protocol)
        if result is not None:
            preds.append(EvalPrediction(
                id=c["id"],
                predicted_intent=result.intent,
                predicted_fields=result.fields,
            ))
        else:
            preds.append(EvalPrediction(
                id=c["id"],
                predicted_intent="chat.ignore",
                predicted_fields={},
            ))

    report = evaluate(cases, preds)
    # 关键词匹配器对显式关键词样本应达到 100% 意图准确率
    keyword_cases = [c for c in raw_cases if c.get("has_keyword")]
    if keyword_cases:
        for kc in keyword_cases:
            assert any(
                p.id == kc["id"] and p.predicted_intent == kc["expected_intent"]
                for p in preds
            ), f"关键词用例 {kc['id']} 应命中: {kc['expected_intent']}"
    # 至少不应崩溃
    assert report.intent_accuracy >= 0.0
