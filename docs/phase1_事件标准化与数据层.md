# Phase 1 验证记录 — 事件标准化与数据层

> 日期：2026-08-11
> 状态：v3 基础数据层 8 项完成；作为 v4 历史基线保留，数据模型迁移待 Task 5
> 测试群：钉钉消息测试

## 1. 完成项总览

| # | 清单项 | 状态 | 产出 |
|---|---|---|---|
| 1 | 脱敏消息样本库 | ✅ | `tests/fixtures/events.json`（17 条样本，覆盖 3 类） |
| 2 | 群与角色配置 | ✅ | `config.py`（已录入测试群角色） |
| 3 | 9 张表一次性建全 | ✅ | `db.py` init_schema（9 表 + 11 索引） |
| 4 | 事件监听器 | ✅ | `event_listener.py`（多群长连接 + 断线重连） |
| 5 | 标准化、过滤、角色识别 | ✅ | `event_normalizer.py` + `role_resolver.py` |
| 6 | 幂等表 | ✅ | `processed_events` 表（业务处理同事务） |
| 7 | 角色重叠校验 + 乱序保护 | ✅ | `role_resolver.py` + `ordering.py` |
| 8 | 测试 | ✅ | `tests/` 下 57 个用例全部通过 |

## 2. 样本库（`tests/fixtures/events.json`）

基于 53 条历史记录脱敏，覆盖 3 类 17 条：

- **文本样本 9 条**：店长（#报修/完毕/普通沟通）、工程师（#故障判断/#维修方式/#超时原因）、其他成员、系统账号回流、未知成员
- **媒体样本 3 条**：图片/文件/富文本（⚠️ 字段为推测，待真实事件样本校准）
- **异常样本 5 条**：缺字段、时间非法、重复 message_id

## 3. 角色配置

| 角色 | 成员 | userId |
|---|---|---|
| 店长 | yushui | 外部账号（测试占位 yushui-external-test） |
| 工程师 | 聂宇清 | 1785387642795212 |
| 工程负责人 | 聂宇清（暂代） | 1785387642795212 |
| 区域经理 | 聂宇清（暂代） | 1785387642795212 |

## 4. 数据库（`data/tickets.db`，v3 历史基线）

9 张表均已创建，11 个索引（含 2 个部分唯一索引），关键约束：

- `tickets` 部分唯一索引：每群最多一张 ACTIVE/ACTIVE_OVERDUE 工单。该约束属于 v3 遗留设计，v4 Task 5 必须通过正式迁移删除，不能作为 v4 完成项
- `processed_events` UNIQUE ON `(message_id, conversation_id)`
- `tickets.ticket_seq` 不被 upsert 重置，不同群 seq 独立
- 回滚时 `processed_events` 一并撤销（业务处理与幂等写入同事务）

## 5. 真实冒烟验证（2026-08-11）

两次验证：首次发现 2 个 bug 并修复，最终全链路跑通。

### 5.1 最终验证结果

```
群消息发送 → 成功
监听就绪 → 5 秒内 [event] ready
收到消息 → sender=聂宇清 role=ENGINEER
角色识别 → ENGINEER（符合 config.py 配置）
私信回执 → 成功（--user userId 方式）
```

### 5.2 发现并修复的 Bug

| Bug | 根因 | 修复 |
|---|---|---|
| ready 标记检测失败 | dws 的 `[event] ready` 打在 stderr，原代码只读 stdout | 新增 `_drain_stderr` 消费 stderr，解析 ready/exited |
| 取消任务后残留监听进程 | cancel 时 `finally` 只置 `_proc=None`，未杀子进程 | 新增 `_terminate_proc()`，CancelledError 与 finally 都调用 |
| 共享 bus 进程订阅污染 | Phase 0 起的旧 bus 进程被残留监听污染，收不到新事件 | 重启 bus 进程解决（环境层面，正式部署需注意进程生命周期） |

### 5.3 额外补齐

- `_dispatch_line` 增加「收到原始事件」DEBUG 日志（event_id/type/message_id）
- `message_already_seen` 增加「幂等命中跳过」INFO 日志

## 6. 测试覆盖（2026-08-11 历史基线）

```
tests/
├── test_event_normalizer.py   （原 13 个，Phase 0 已有）
├── test_role_permissions.py   （原 4 个，Phase 0 已有）
├── test_fixture_samples.py    （新增 19 个，样本库驱动）
├── test_db.py                 （新增 13 个，幂等事务/部分唯一索引/upsert）
└── test_ordering.py           （新增 8 个，乱序保护基础）
────────────────────────────────
  57 passed（0 failed）
```

截至 2026-08-12，加入 v4 语义协议、关键词、模型契约和评测测试后，仓库全量基线为 `119 passed`。本节的 57 项仅记录 Phase 1 当日验收范围。

## 7. 关键发现（影响后续 Phase）

1. **dws 事件流分工**：`[event] ready` / `[event] exited` 在 stderr，事件数据在 stdout。监听器必须同时消费双流。
2. **共享 bus 进程**：`subscribe_id` 为固定派生值，bus 进程跨监听会话共享。残留进程会污染订阅注册。
3. **私聊参数**：企业内部成员用 `--user <userId>`；`--open-dingtalk-id` 仅外部/机器人/跨组织身份。yushui（外部账号）私聊不可达，正式环境无此限制。
4. **角色重叠校验**：`role_resolver` 启动即检查，同一 userId 不允许多角色——这不应绕过。
5. **yushui 私聊不可达**：Phase 4 升级私发测试如需真实验证，需要组织内店长成员。
6. **v4 多工单迁移未完成**：当前数据库仍保留单群单活动工单唯一索引；在 Task 5 完成前，不得进行同群多活动工单验收。
7. **媒体字段仍为推测样本**：图片、文件和富文本必须用真实事件重新校准，并在附件任务中验证下载、摘要和落盘。
8. **ID 映射仍为静态配置**：正式环境需在启动阶段通过通讯录解析并缓存，作为上线前置条件处理。

## 8. 开发速查

```bash
# 发起监听（带 group 过滤，120s 窗口）
python event_listener.py --duration 120 --group "钉钉消息测试"

# 发群消息
dws chat message send --group <openConversationId> --text "内容"

# 发私聊
dws chat message send --user <userId> --text "内容"

# 运行全量测试
python -m pytest tests/ -q
```
