# Task 4 OpenAI-Compatible 云端模型接入设计

日期：2026-08-12

## 目标

完成计划书 Task 4：通过 OpenAI-compatible Chat Completions 接口识别无显式关键词的消息，输出受协议约束的 `SemanticDecision`，并使用已配置模型完成真实盲测。

## 请求策略

- 显式关键词继续走 `match_keyword`，不调用云端模型。
- 自然语言分类每次只发送一次 HTTP 请求；客户端内部不重试，重试由后续 Inbox Worker 负责。
- 默认超时时间为 60 秒，可由 `LLM_TIMEOUT_SECONDS` 覆盖。
- 请求使用 `POST {LLM_BASE_URL}/chat/completions`。
- 请求头携带 Bearer Token、请求追踪 ID 和幂等键；日志不得输出 API Key 或 Authorization 头。

## 响应格式兼容

新增 `LLM_RESPONSE_FORMAT`：

- `json_schema`：使用 `response_format.type=json_schema` 和固定输出 Schema。
- `json_object`：使用 `response_format.type=json_object`，响应后执行严格本地校验。
- `auto`：OpenAI 官方地址选择 `json_schema`，其他 OpenAI-compatible 地址选择 `json_object`。

不允许在一次分类过程中先尝试 `json_schema`、失败后再发第二次请求降级，以保证单次 HTTP 调用契约。

## 分类输入

模型输入仅包含完成当前语义判断所需信息：

- 当前协议版本及启用的动作子集；
- 每个动作的角色、字段、目标策略和确认策略；
- 当前消息文本、发送人角色；
- 当前群候选工单的编号、主题、位置、摘要、状态和版本；
- 当前用户待确认动作的受限摘要；
- 固定 JSON 输出规则和禁止猜测要求。

不发送 API Key、数据库连接信息或无关历史消息。

## 本地安全校验

无论服务端使用哪种响应格式，均执行：

- 响应必须是 JSON 对象；
- intent 必须在协议 allowlist 中；
- 字段必须属于动作 required/optional 字段白名单；
- 枚举值必须符合协议和本地规则；
- 目标工单编号和候选评分只能引用当前候选；
- 置信度限制在 `[0, 1]`；
- 高风险及 `requires_confirmation` 结果交由 validator 应用确认策略；
- 超时、HTTP 错误、非 JSON、Schema 错误或协议外输出返回安全降级决策，不自动执行未知动作。

## 离线评测入口

扩展 `semantics.evaluator`：

```bash
python -m semantics.evaluator \
  --live-model \
  --dataset tests/fixtures/semantic_cases.blind.json
```

- 无 `--live-model` 时运行确定性关键词基线。
- 有 `--live-model` 时加载 `.env`、协议和模型客户端，逐条调用分类器并输出完整指标。
- 真实评测必须使用盲测集冻结标签，不修改标签以迎合模型结果。

## 测试与验收

契约测试覆盖：

- 60 秒默认超时和配置覆盖；
- `json_schema`、`json_object`、`auto` 三种模式；
- 每次分类最多一次 HTTP 调用；
- 超时、HTTP 异常、非 JSON、协议外 intent；
- 字段幻觉、非法枚举、越界置信度；
- 候选工单和评分越界；
- payload 包含协议子集、候选摘要和待确认上下文；
- 日志和异常不泄露 API Key；
- 模块 CLI 的关键词和真实模型模式。

最终运行：

```bash
python -m pytest tests/test_model_contract.py -q
python -m pytest tests -q
python -m semantics.evaluator --live-model \
  --dataset tests/fixtures/semantic_cases.blind.json
```

## 提交策略

设计、实现、测试和必要文档在 Task 4 全部验收后合并为一个 Git 提交。

## 验收结果

- 模型契约测试：27 条通过。
- 全量自动化测试：151 条通过。
- 已配置 OpenAI-compatible 模型冻结盲测：意图准确率 100%，建单精确率 100%，建单召回率 100%，误建单率 0%。
- 盲测字段准确率为 40%，主要来自模型抽取文本与人工标准答案的措辞差异，不影响本次接口与安全契约验收。
