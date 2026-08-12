"""事件标准化：dws NDJSON 事件 → NormalizedMessage。

- 缺失 message_id / group_id / sender_id / sent_at 的事件直接丢弃（WARNING 日志）。
- 系统账号（工程部AI）回流消息标记 is_self，业务层忽略。
- sender_id 统一取 sender_open_dingtalk_id（Phase 0 实测字段）。
- create_time 为本地时区字符串（如 "2026-08-11 10:36:01"），解析为 datetime。
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Optional

from config import LISTENER_USER_ID
from logger import get_logger
from models import MSG_FILE, MSG_IMAGE, MSG_RICH, MSG_TEXT, NormalizedMessage
from role_resolver import resolve_role

logger = get_logger(__name__)

# 媒体消息类型映射（content 为可读描述；真实字段待样本验证，Phase 1 待办）
_KNOWN_MEDIA_TYPES = {"image", "file", "audio", "video", "rich"}

# 富文本可读字段优先级（dws 富文本常见结构；待真实样本确认）
_RICH_TEXT_KEYS = ("markdown", "text", "content")

_REQUIRED_FIELDS = ("message_id", "conversation_id", "sender_open_dingtalk_id")

_TIME_FORMATS = (
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M:%S.%f",
    "%Y-%m-%dT%H:%M:%S",
)


def _parse_sent_at(raw: Any) -> Optional[datetime]:
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        # 毫秒时间戳（event_time），转 datetime
        try:
            return datetime.fromtimestamp(raw / 1000)
        except (OverflowError, OSError, ValueError):
            return None
    s = str(raw).strip()
    for fmt in _TIME_FORMATS:
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    logger.warning("sent_at 解析失败 value=%r", raw)
    return None


def _detect_message_type(event: dict[str, Any]) -> str:
    """按事件可选字段判断消息类型；缺省 text。

    TODO(Phase 1 待办)：图片/文件/富文本事件的真实字段待采集样本后完善。
    """
    for key in ("msg_type", "msgtype", "type"):
        v = event.get(key)
        if isinstance(v, str) and v in _KNOWN_MEDIA_TYPES:
            return v
    return MSG_TEXT


def _extract_rich_text(event: dict[str, Any]) -> str:
    """富文本事件的可读文本抽取。

    content 可能为 JSON 字符串（含 markdown/text 字段）或纯文本。
    真实结构待样本验证；当前做宽松兼容，无法解析时返回原值。
    """
    content = event.get("content")
    if not content:
        return ""
    if isinstance(content, dict):
        for key in _RICH_TEXT_KEYS:
            v = content.get(key)
            if isinstance(v, str):
                return v
        return json.dumps(content, ensure_ascii=False)
    if isinstance(content, str):
        s = content.strip()
        if s.startswith("{") and s.endswith("}"):
            try:
                obj = json.loads(s)
            except json.JSONDecodeError:
                return s
            if isinstance(obj, dict):
                for key in _RICH_TEXT_KEYS:
                    v = obj.get(key)
                    if isinstance(v, str):
                        return v
                return json.dumps(obj, ensure_ascii=False)
        return s
    return str(content)


def _extract_reply_to(raw_event: dict[str, Any]) -> Optional[str]:
    """v4.0：从原始事件提取钉钉回复/引用目标消息 ID。

    TODO(Phase 1 待验证)：以下候选字段需用真实 NDJSON 样本确认。
    dws listen-im 的引用消息常见字段包括 reply_to_message_id、
    original_message_id、quote_message_id 或 refer_message_id。
    当前按优先顺序试探，后续以真实样本为准。
    """
    for key in ("reply_to_message_id", "original_message_id", "quote_message_id", "refer_message_id"):
        v = raw_event.get(key)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return None


def normalize_event(
    raw_event: dict[str, Any],
    group: Optional[dict] = None,
    id_map: Optional[dict[str, str]] = None,
) -> Optional[NormalizedMessage]:
    """原始事件 → NormalizedMessage；无法标准化返回 None（已打日志）。

    参数：
        raw_event: dws event NDJSON 的一行（已 json.loads）。
        group: 群配置（用于角色解析），可为 None。
        id_map: openDingtalkId → userId 映射（角色识别用）。
    """
    for field in _REQUIRED_FIELDS:
        if not raw_event.get(field):
            logger.warning(
                "事件标准化失败：缺少必填字段 field=%s event_id=%s",
                field, raw_event.get("event_id"),
            )
            return None

    message_id = raw_event["message_id"]
    group_id = raw_event["conversation_id"]
    sender_id = raw_event["sender_open_dingtalk_id"]
    sender_name = raw_event.get("sender") or sender_id
    message_type = _detect_message_type(raw_event)
    if message_type == MSG_RICH:
        content = _extract_rich_text(raw_event)
    elif message_type in (MSG_IMAGE, MSG_FILE):
        content = raw_event.get("content") or f"[{message_type}]"
    else:
        content = raw_event.get("content") or ""
    sent_at = _parse_sent_at(raw_event.get("create_time") or raw_event.get("event_time"))
    reply_to_message_id = _extract_reply_to(raw_event)

    if sent_at is None:
        logger.warning(
            "事件标准化失败：sent_at 不可用 message_id=%s", message_id,
        )
        return None

    is_self = sender_id == LISTENER_USER_ID

    msg = NormalizedMessage(
        message_id=message_id,
        group_id=group_id,
        sender_id=sender_id,
        sender_name=sender_name,
        content=content,
        message_type=message_type,
        sent_at=sent_at,
        is_self=is_self,
        reply_to_message_id=reply_to_message_id,
        raw_event=raw_event,
    )
    msg.sender_role = resolve_role(group, sender_id, id_map) if not is_self else "SYSTEM"

    if is_self:
        logger.info("系统账号回流消息过滤 sender=%s %s", sender_name, msg.brief())
    else:
        logger.debug("事件标准化完成 role=%s %s", msg.sender_role, msg.brief())
    return msg
