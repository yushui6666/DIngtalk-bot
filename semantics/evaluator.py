"""离线评测器（计划书 Task 3）。

对关键词匹配器或模型分类器的输出做结构化评测，统计意图、字段、
建单、自动路由和歧义澄清指标。模块入口提供无需网络的关键词基线评测。
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class LabeledCase:
    """人工标签的一条测试用例。"""

    id: str
    text: str
    expected_intent: str
    expected_fields: dict[str, Any] = field(default_factory=dict)
    expected_ticket_no: str | None = None


@dataclass(frozen=True)
class EvalPrediction:
    """匹配器或分类器对一条消息的预测。"""

    id: str
    predicted_intent: str
    predicted_fields: dict[str, Any] = field(default_factory=dict)
    predicted_ticket_no: str | None = None


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
    routing_precision: float
    ambiguity_clarification_rate: float
    per_class: dict[str, dict[str, int]] = field(default_factory=dict)

    def __str__(self) -> str:
        return (
            f"EvaluationReport(total={self.total_cases}, matched={self.matched_cases}, "
            f"intent_acc={self.intent_accuracy:.2%}, field_acc={self.field_accuracy:.2%}, "
            f"create_prec={self.create_precision:.2%}, create_rec={self.create_recall:.2%}, "
            f"false_create={self.false_create_rate:.2%}, routing_prec={self.routing_precision:.2%}, "
            f"ambiguity_clarify={self.ambiguity_clarification_rate:.2%})"
        )


def _index_unique(items: list[Any], *, kind: str) -> dict[str, Any]:
    indexed: dict[str, Any] = {}
    for item in items:
        if item.id in indexed:
            raise ValueError(f"重复{kind} ID: {item.id}")
        indexed[item.id] = item
    return indexed


def evaluate(
    cases: list[LabeledCase],
    predictions: list[EvalPrediction],
) -> EvaluationReport:
    """按用例 ID 对齐预测并计算离线评测指标。"""
    case_by_id = _index_unique(cases, kind="用例")
    pred_by_id = _index_unique(predictions, kind="预测")
    unexpected_ids = sorted(set(pred_by_id) - set(case_by_id))
    if unexpected_ids:
        raise ValueError(f"未知预测 ID: {', '.join(unexpected_ids)}")

    total = len(cases)
    matched = 0
    intent_correct = 0
    field_correct_count = 0
    field_total_count = 0
    create_true_positive = 0
    create_false_positive = 0
    create_false_negative = 0
    clarify_expected = 0
    clarify_correct = 0
    routed_predictions = 0
    correctly_routed_predictions = 0
    per_class: dict[str, dict[str, int]] = {}

    for case in cases:
        exp = case.expected_intent
        pred = pred_by_id.get(case.id)
        pred_intent = pred.predicted_intent if pred else None

        stats = per_class.setdefault(exp, {"total": 0, "correct": 0})
        stats["total"] += 1
        if pred is not None:
            matched += 1
        if pred_intent == exp:
            intent_correct += 1
            stats["correct"] += 1

        predicted_fields = pred.predicted_fields if pred else {}
        field_names = set(case.expected_fields) | set(predicted_fields)
        for field_name in field_names:
            field_total_count += 1
            if (
                pred_intent == exp
                and
                field_name in case.expected_fields
                and field_name in predicted_fields
                and _field_values_equal(
                    case.expected_fields[field_name], predicted_fields[field_name]
                )
            ):
                field_correct_count += 1

        if exp == "ticket.create":
            if pred_intent == "ticket.create":
                create_true_positive += 1
            else:
                create_false_negative += 1
        elif pred_intent == "ticket.create":
            create_false_positive += 1

        if exp == "system.clarify":
            clarify_expected += 1
            if pred_intent == "system.clarify":
                clarify_correct += 1

        if pred and pred.predicted_ticket_no is not None:
            routed_predictions += 1
            if pred.predicted_ticket_no == case.expected_ticket_no:
                correctly_routed_predictions += 1

    create_precision_denominator = create_true_positive + create_false_positive
    create_recall_denominator = create_true_positive + create_false_negative

    return EvaluationReport(
        total_cases=total,
        matched_cases=matched,
        intent_accuracy=intent_correct / total if total else 0.0,
        field_accuracy=(
            field_correct_count / field_total_count if field_total_count else 0.0
        ),
        create_precision=(
            create_true_positive / create_precision_denominator
            if create_precision_denominator
            else 0.0
        ),
        create_recall=(
            create_true_positive / create_recall_denominator
            if create_recall_denominator
            else 0.0
        ),
        false_create_rate=create_false_positive / total if total else 0.0,
        routing_precision=(
            correctly_routed_predictions / routed_predictions if routed_predictions else 0.0
        ),
        ambiguity_clarification_rate=(
            clarify_correct / clarify_expected if clarify_expected else 0.0
        ),
        per_class=per_class,
    )


def _field_values_equal(expected: Any, predicted: Any) -> bool:
    """比较字段值是否相等（列表忽略顺序但保留重复次数）。"""
    if isinstance(expected, list) and isinstance(predicted, list):
        return sorted(map(str, expected)) == sorted(map(str, predicted))
    return str(expected).strip() == str(predicted).strip()


def _load_dataset(path: Path) -> list[dict[str, Any]]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, list):
        raise ValueError("数据集根节点必须是数组")
    return [case for case in raw if isinstance(case, dict) and "id" in case]


def _run_keyword_baseline(dataset: list[dict[str, Any]]) -> EvaluationReport:
    from semantics.keyword_matcher import match_keyword
    from semantics.protocol_loader import load_protocol

    protocol_path = Path(__file__).resolve().parent.parent / "protocols" / "ticket_semantics.v4.json"
    protocol = load_protocol(protocol_path)
    cases: list[LabeledCase] = []
    predictions: list[EvalPrediction] = []
    for raw_case in dataset:
        cases.append(
            LabeledCase(
                id=raw_case["id"],
                text=raw_case["text"],
                expected_intent=raw_case["expected_intent"],
                expected_fields=raw_case.get("expected_fields", {}),
                expected_ticket_no=raw_case.get("expected_ticket_no"),
            )
        )
        decision = match_keyword(raw_case["text"], protocol)
        predictions.append(
            EvalPrediction(
                id=raw_case["id"],
                predicted_intent=decision.intent if decision else "chat.ignore",
                predicted_fields=dict(decision.fields) if decision else {},
                predicted_ticket_no=decision.target_ticket_no if decision else None,
            )
        )
    return evaluate(cases, predictions)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="运行确定性关键词语义离线评测")
    parser.add_argument("--dataset", type=Path, required=True, help="标注数据集 JSON 路径")
    args = parser.parse_args(argv)
    report = _run_keyword_baseline(_load_dataset(args.dataset))
    print(json.dumps({
        "total_cases": report.total_cases,
        "matched_cases": report.matched_cases,
        "intent_accuracy": report.intent_accuracy,
        "field_accuracy": report.field_accuracy,
        "create_precision": report.create_precision,
        "create_recall": report.create_recall,
        "false_create_rate": report.false_create_rate,
        "routing_precision": report.routing_precision,
        "ambiguity_clarification_rate": report.ambiguity_clarification_rate,
        "per_class": report.per_class,
    }, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
