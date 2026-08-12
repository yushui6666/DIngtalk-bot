# Phase 0 验证记录 — 环境与 dws 通道

> 日期：2026-08-11
> 状态：核心通道验证通过；多群监听代码能力已完成，真实多群并行验证暂缓
> 测试群：钉钉消息测试（用户指定）

## 1. 环境确认

| 项目 | 值 |
|---|---|
| dws CLI | v1.0.57 (6811a123, 2026-08-06) |
| 登录账号 | 工程部AI（`ding25903599fd309ac9:17848633777275584`） |
| 组织 | 杭州欢愉商业经营管理有限公司 |
| 登录态 | active，refreshExpAt 2026-09-10（token 可自动续期） |

系统监听账号 = 「工程部AI」，与设计一致。

## 2. 测试群信息

- 群名：**钉钉消息测试**
- openConversationId：`cidO+6f66Jja9EzGTFm3rra1Q==`
- 成员：
  - yushui（群主）`Dk7Rf4NfFahnD2MHQgAE3gy2iPTIiiIm8jw`
  - 聂宇清 `DV2iipykTJciSappVW4GfsiSQii2iPTIiiIm8jw`
  - 工程部AI（系统账号）`DuT5LjNZRjS6gMdv9dii9LLC2iPTIiiIm8jw`
  - 小钉（机器人）`REwO84KtLYF6qUufAZxzig==`

## 3. 通道验证结果

| # | 验证项 | 命令 | 结果 |
|---|---|---|---|
| 1 | 群消息监听 | `dws event +listen-im --kind group --chat-query "钉钉消息测试" -f ndjson --duration 150s` | ✅ 收到 2 条真实事件 |
| 2 | 群消息发送 | `dws chat message send --group <openConversationId> --text "..."` | ✅ SUCCESS（openMessageId 可查） |
| 3 | 私聊发送（组织内成员） | `dws chat message send --user <userId> --text "..."` | ✅ SUCCESS（聂宇清 userId=1785387642795212） |
| 4 | 私聊发送（open-dingtalk-id 方式） | `dws chat message send --open-dingtalk-id <openDingtalkId> --text "..."` | ❌ FAILED（企业内部成员不适用此参数） |
| 5 | 多群并行监听 | 代码支持多群监听、断线重连和优雅清理；真实多群并行通道验证暂缓 | ⏸ |

发送均返回 `openTaskId`，用 `dws chat message query-send-status --open-task-id <id>` 查询真实状态（sendStatus=SUCCESS/FAILED）。

### 私聊通道结论（重要）

- **企业内部成员单聊必须用 `--user <userId>`**；`--open-dingtalk-id` 只适用于外部联系人/机器人/跨组织身份，企业内部成员会 FAILED。
- 测试群成员 `yushui` 为**外部测试账号，未加入任何组织**（通讯录搜索不到、无 userId、单聊不可达）；用户确认正式应用时群成员均为统一组织成员，该限制不影响正式环境。
- 由此确定的 ID 策略：**角色配置与私聊发送统一用 userId；事件只提供 openDingtalkId，运行时经 openDingtalkId→userId 映射识别角色**（见 `id_mapper.py`）。

## 4. NDJSON 事件格式（真实样例）

```json
{
  "type": "user_im_message_receive_group",
  "event_id": "fec9a70f2ddd43ad9de29b8b4ac86733",
  "timestamp": 1786415762243,
  "subscribe_id": "subId-c1973b128cb643968b07a40e315e376a",
  "message_id": "msg0F8pMh9Quen8TndDnKl9+Q==",
  "conversation_id": "cidO+6f66Jja9EzGTFm3rra1Q==",
  "sender": "聂宇清",
  "sender_open_dingtalk_id": "DV2iipykTJciSappVW4GfsiSQii2iPTIiiIm8jw",
  "content": "收到",
  "create_time": "2026-08-11 10:36:01",
  "event_time": 1786415761476
}
```

字段 → `NormalizedMessage` 映射（Phase 1 依据）：

| 事件字段 | 说明 | 映射目标 |
|---|---|---|
| `message_id` | 钉钉消息 ID，幂等唯一键 | `message_id` |
| `conversation_id` | 群 openConversationId | `group_id` |
| `sender_open_dingtalk_id` | 发送人 openDingtalkId | `sender_id` |
| `sender` | 发送人昵称 | `sender_name` |
| `content` | 消息正文（文本）；媒体消息为可读描述 | `content` |
| `create_time` | 发送时间（本地时区字符串） | `sent_at` |
| `type` | 事件类型 | 用于过滤 |
| `event_id` / `timestamp` | 事件投递标识 | 日志/去重辅助 |

## 5. 关键发现（影响 Phase 1 设计）

1. **事件只提供 `sender_open_dingtalk_id`，不是 userId。** 群成员列表（`+chat-members-list`）同样只返回 openDingtalkId。
   → **角色配置（groups 表 manager_ids/engineer_ids 等）与私聊发送统一使用 userId**，运行时通过 openDingtalkId→userId 映射识别角色（`id_mapper.py` + `role_resolver.py`，Phase 1 已落地）。
2. **私聊发送参数**：企业内部成员用 `--user <userId>`；`--open-dingtalk-id` 仅外部/机器人/跨组织。用错会 FAILED（实测验证）。
3. 自己发的消息不会作为事件回来（isSelfLoop 过滤）——监听测试必须由他人发消息，正式系统天然不会处理自己发送的系统消息，与设计「系统账号过滤」一致。
4. 监听进程退出行为：`--duration` 超时或 `--max-events` 达到后自动退订并退出（exit 0）；ready marker 为 `[event] ready event_key=...`。
5. 外部账号（如 yushui）不在通讯录、无 userId、单聊不可达；正式环境统一组织，不受影响。

## 6. 开发速查（命令基线）

```bash
# 搜索群
dws chat search --query "群名" --format json

# 群成员列表（用户/机器人分桶）
dws chat +chat-members-list --conversation-id <cid> --format json

# 群消息监听（NDJSON，--duration/--max-events 自动退出）
dws event +listen-im --kind group --chat-query "群名" -f ndjson --duration 150s

# 发群消息
dws chat message send --group <openConversationId> --text "内容"

# 发私聊（组织内成员，userId）
dws chat message send --user <userId> --text "内容"

# 查询发送状态
dws chat message query-send-status --open-task-id <openTaskId>
```

## 7. 待办

- [ ] 多群并行监听真实验证（代码能力已完成，生产通道验收仍待执行）
- [ ] 图片 / 文件 / 富文本真实事件字段采集（转入附件与视觉模型任务）
- [ ] 验证 `chat message download-media`、文件校验和原子落盘（转入附件与视觉模型任务）
- [ ] `id_mapper` 正式化：启动时调用通讯录接口自动解析 userId→openDingtalkId 并缓存（上线前置条件）

> Phase 0 不需要代码返工；以上事项属于生产通道和媒体能力验收，不能因为监听代码已支持而视为实测完成。
