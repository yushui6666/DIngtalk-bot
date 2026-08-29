"""隐式反馈比对（v4.3 任务 7）：建议原因 vs 工程师实际诊断。

零打扰设计：工程师提交 #故障判断 时自动比对落库，不需要任何人
多打一个字。命中 → 案例排序可加权；偏差 → 冷启动校准信号。

比对规则（确定性，不调模型）：
- 诊断项与建议原因任一「互相包含」或共享 ≥2 个连续字符片段 → 命中；
- 文本先做空白/标点归一化。
"""

from __future__ import annotations

import re
from typing import Any

_UNICODE_PUNCT = re.compile(r"[\s，。；、！？：,.;:!?\-—/\\()（）]+")


def _normalize(text: str) -> str:
    return _UNICODE_PUNCT.sub("", str(text or "")).lower()


def _overlaps(a: str, b: str, *, min_seg: int = 2) -> bool:
    """两段归一化文本是否存在 ≥min_seg 连续字符的公共子串（双向包含也命中）。"""
    na, nb = _normalize(a), _normalize(b)
    if not na or not nb:
        return False
    if na in nb or nb in na:
        return True
    shorter, longer = (na, nb) if len(na) <= len(nb) else (nb, na)
    for i in range(len(shorter) - min_seg + 1):
        if shorter[i:i + min_seg] in longer:
            return True
    return False


def compare_suggestion_with_diagnosis(
    suggestion: dict[str, Any], diagnosis_items: list[str]
) -> dict[str, Any]:
    """比对一条建议与工程师诊断项，返回可落库的结果结构。"""
    detail = suggestion.get("detail") or {}
    causes = [str(c) for c in (detail.get("causes") or [])]
    hit_items = [
        item for item in diagnosis_items
        if any(_overlaps(item, cause) for cause in causes)
    ]
    return {
        "hit": bool(hit_items) if causes else False,
        "matched_items": hit_items,
        "causes": causes,
        "diagnosis": list(diagnosis_items),
    }
