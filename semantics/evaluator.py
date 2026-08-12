"""离线评测器（计划书 Task 3）。

对关键词匹配器或模型分类器的输出做结构化评测，统计：
- intent_accuracy: 意图准确率
- field_accuracy: 字段抽取准确率（预期字段中预测正确的比例）
- create_precision / create_recall / false_create_rate: 建单相关指标
- ambiguity_clarification_rate: 歧义澄清率

可用于：
- 关键词匹配器基线评测
- 模型分类器离线评测
- 盲测集最终验收
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


# ───────────────────────── 数据类 ─────────────────────────


@dataclass(frozen=True)
class LabeledCase:
    """人工标签的一条测试用例。"""
    id: str
    text: str
    expected_intent: str
    expected_fields: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class EvalPrediction:
    """匹配器或分类器对一条消息的预测。"""
    id: str
    predicted_intent: str
    predicted_fields: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class EvaluationReport:
    """评测报告（不可变）。"""
    total_cases: int
    matched_cases: int
    intent_accuracy: float
    field_accuracy: float
    create_precision: float
    create_recall: float
    false_create_rate: float
    ambiguity_clarification_rate: float
    per_class: dict[str, dict[str, int]] = field(default_factory=dict)

    def __str__(self) -> str:
        return (
            f"EvaluationReport(total={self.total_cases}, matched={self.matched_cases}, "
            f"intent_acc={self.intent_accuracy:.2%}, field_acc={self.field_accuracy:.2%}, "
            f"create_prec={self.create_precision:.2%}, create_rec={self.create_recall:.2%}, "
            f"false_create={self.false_create_rate:.2%}, ambiguity_clarify={self.ambiguity_clarification_rate:.2%})"
        )


# ───────────────────────── 评测逻辑 ─────────────────────────


def evaluate(
    cases: list[LabeledCase],
    predictions: list[EvalPrediction],
) -> EvaluationReport:
    """按标注用例和预测结果计算评测指标。

    Args:
        cases: 人工标签用例列表。
        predictions: 预测结果列表（应与 cases 一一对应，按 id 匹配）。

    Returns:
        EvaluationReport 含所有指标。
    """
    if not cases:
        return EvaluationReport(
            total_cases=0, matched_cases=0,
            intent_accuracy=0.0, field_accuracy=0.0,
            create_precision=0.0, create_recall=0.0,
            false_create_rate=0.0, ambiguity_clarification_rate=0.0,
        )

    # 建立预测索引
    pred_by_id: dict[str, EvalPrediction] = {p.id: p for p in predictions}

    total = len(cases)
    matched = 0  # 有对应预测的用例数

    # 意图统计
    intent_correct = 0
    field_correct_count = 0
    field_total_count = 0

    # 建单统计
    create_true_positive = 0  # 预期 create，预测 create
    create_false_positive = 0  # 预期非 create，预测 create
    create_false_negative = 0  # 预期 create，预测非 create

    # 歧义澄清
    clarify_expected = 0  # 预期 system.clarify
    clarify_predicted = 0  # 预测 system.clarify
    clarify_correct = 0  # 预期且预测正确

    per_class: dict[str, dict[str, int]] = {}

    for case in cases:
        pred = pred_by_id.get(case.id)
        if pred is None:
            # 无匹配预测
            if case.expected_intent == "ticket.create":
                create_false_negative += 1
            continue

        matched += 1
        exp = case.expected_intent
        pred_intent = pred.predicted_intent

        # 意图正确
        if exp == pred_intent:
            intent_correct += 1

        # 按类别统计
        if exp not in per_class:
            per_class[exp] = {"total": 0, "correct": 0}
        per_class[exp]["total"] += 1
        if exp == pred_intent:
            per_class[exp]["correct"] += 1

        # 字段准确率
        for fname, expected_value in case.expected_fields.items():
            field_total_count += 1
            predicted_value = pred.predicted_fields.get(fname)
            if _field_values_equal(expected_value, predicted_value):
                field_correct_count += 1

        # 建单指标
        if exp == "ticket.create":
            if pred_intent == "ticket.create":
                create_true_positive += 1
            else:
                create_false_negative += 1
        else:
            if pred_intent == "ticket.create":
                create_false_positive += 1

        # 歧义澄清
        if exp == "system.clarify":
            clarify_expected += 1
            if pred_intent == "system.clarify":
                clarify_correct += 1
        if pred_intent == "system.clarify":
            clarify_predicted += 1

    # 计算指标
    intent_accuracy = intent_correct / total if total > 0 else 0.0
    field_accuracy = field_correct_count / field_total_count if field_total_count > 0 else 0.0

    create_denom_prec = create_true_positive + create_false_positive
    create_precision = create_true_positive / create_denom_prec if create_denom_prec > 0 else 0.0

    create_denom_rec = create_true_positive + create_false_negative
    create_recall = create_true_positive / create_denom_rec if create_denom_rec > 0 else 0.0

    false_create_rate = create_false_positive / total if total > 0 else 0.0

    ambiguity_clarification_rate = clarify_correct / clarify_expected if clarify_expected > 0 else 0.0

    return EvaluationReport(
        total_cases=total,
        matched_cases=matched,
        intent_accuracy=intent_accuracy,
        field_accuracy=field_accuracy,
        create_precision=create_precision,
        create_recall=create_recall,
        false_create_rate=false_create_rate,
        ambiguity_clarification_rate=ambiguity_clarification_rate,
        per_class=per_class,
    )


def _field_values_equal(expected: Any, predicted: Any) -> bool:
    """比较字段值是否相等（列表则比较集合）。"""
    if isinstance(expected, list) and isinstance(predicted, list):
        return set(expected) == set(predicted)
    return str(expected).strip() == str(predicted).strip()
