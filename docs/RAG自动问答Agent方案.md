# RAG 自动问答 Agent 接入方案 — 研究与设计

> 版本：v1.0（研究稿，待确认后实施）  
> 日期：2026-08-20  
> 目标：让门店群里的咨询类问题由机器人基于知识库自动回答，不再依赖工程师人工解答  
> 定位：v4.x 新增子项目，遵循既有原则「大模型只负责理解，规则引擎负责执行」

---

## 一、现状与问题

### 1.1 当前系统对「咨询类消息」的处理

```
用户："空调不制冷一般是什么原因？" / "报修错了怎么撤回？"
  → 语义分类器 → chat.ignore（纯笼统询问）
  → 群内无任何动作，等待工程师人工解答
```

系统已有能力与缺口：

| 能力 | 现状 | RAG 问答需要的 |
|---|---|---|
| 意图理解 | 协议约束的分类器（`semantics/classifier`） | 新增 `qa.answer` 只读意图 |
| 知识资产 | SQLite 工单库（故障判断/维修方式版本表）、业务文档 | 结构化语料 + 向量索引 |
| 检索 | 无（候选工单是精确匹配，非语义检索） | 混合检索（关键词 + 向量） |
| 生成 | 仅结构化 JSON 输出 | 基于检索片段的自由文本回答 |
| 审计 | `semantic_decisions` 表 | QA 决策同样落审计 |

### 1.2 目标场景（三类问题）

1. **维修经验咨询**：「这种冷柜不制冷一般什么原因？」→ 检索历史工单的故障判断 + 维修方式。
2. **业务操作咨询**：「报修工单怎么取消？」「维修方式要谁填？」→ 检索使用须知 / 业务文档。
3. **状态自查咨询**：「我的工单超时会怎样？」→ 检索 SLA 规则文档（工单实时状态仍走既有 `ticket.query`）。

**非目标**（继续走既有链路）：报修建单、故障上报、工单推进 —— 这些是业务动作，不是问答。

---

## 二、知识语料设计（RAG 的 R）

### 2.1 语料来源

| 来源 | 内容 | 文档粒度 | 更新时机 |
|---|---|---|---|
| **A. 历史工单**（主） | COMPLETED/STOPPED 工单：设备、位置、故障描述、故障判断（diagnosis_versions 当前版）、维修方式（repair_method_versions 当前版）、是否解决 | 一张工单 = 一篇文档 | 工单进入终态时增量重建 |
| **B. 业务文档** | `使用须知.txt`、`docs/*.md`（业务流程、系统说明、测试用例） | 按二级标题 + 段落切块（200~400 字） | 文件 content_hash 变化时重建 |
| **C. 人工 FAQ**（可选） | `data/faq.md`（运营沉淀的高频问答，初期冷启动用） | 一问一答 = 一篇文档 | 人工维护，变更即重建 |

### 2.2 工单文档模板（来源 A 的拼装格式）

```text
【维修案例】冷柜不制冷（工单 W2024-0131，已解决）
门店/空间：XX门店 · 后厨
设备：冷柜（三星）｜位置：后厨
故障描述：冷藏室不制冷，灯亮，压缩机有嗡嗡声
故障判断（工程师）：制冷剂泄漏，冷凝器积灰严重
维修方式：补充制冷剂 + 清洗冷凝器，费用自付
结果：当天修复，7天SLA内完成
```

要点：
- **只收录终态且修复成功**的工单（CANCELLED 的误报单不进知识库，避免污染）；
- 模板固定字段顺序，便于 embedding 稳定命中「现象→原因→处理」语义结构；
- 元数据单独存列（group_id、device、subject、ticket_no、completed_at），供检索后过滤与引用。

### 2.3 存储模型（新增两张表）

```sql
-- 知识文档（一篇案例/一个文档块 = 一行；正文与元数据分离）
CREATE TABLE IF NOT EXISTS kb_documents (
  doc_id      TEXT PRIMARY KEY,          -- 如 ticket:W2024-0131 / doc:使用须知#二 / faq:0042
  source_type TEXT NOT NULL,             -- TICKET_CASE / DOC / FAQ
  title       TEXT NOT NULL,
  content     TEXT NOT NULL,             -- 上文模板正文
  content_hash TEXT NOT NULL,            -- 变更检测，避免重复嵌入
  metadata    TEXT NOT NULL DEFAULT '{}',-- JSON：group_id/device/ticket_no/completed_at...
  embedded_at TEXT,
  is_active   INTEGER NOT NULL DEFAULT 1
);
CREATE INDEX IF NOT EXISTS idx_kbdoc_hash ON kb_documents(content_hash);

-- 向量索引（sqlite-vec vec0 虚表，rowid 对齐 kb_documents）
CREATE VIRTUAL TABLE IF NOT EXISTS kb_vectors USING vec0(
  doc_id TEXT PRIMARY KEY,
  embedding FLOAT[N]                     -- N 由所用模型决定，如 1024/1536
);
```

---

## 三、技术选型（对比与决策）

> 本节基于截至 2025 年中期的技术认知；本次研究时 web 检索接口不可用，未做实时核对，实施前可再验证版本号。

### 3.1 向量存储

| 方案 | 优点 | 缺点 | 结论 |
|---|---|---|---|
| **sqlite-vec**（SQLite 扩展，vec0 虚表） | 与「SQLite 为唯一真相源」完全一致；零独立服务；支持 KNN | 需加载扩展（pip 装sqlite-vec，load_extension） | ✅ **推荐** |
| 纯 Python 余弦（numpy 全量扫） | 零依赖，实现 20 行 | 语料 >5 万条后慢 | ✅ **作为降级路径**（扩展加载失败时自动切换） |
| Chroma / Qdrant / Milvus | 生态成熟 | 单进程 asyncio + SQLite 的部署形态下引入独立服务，过重 | ❌ 不采用 |
| FAISS（本地库） | 性能好 | 原生索引持久化/更新麻烦，依赖重 | ❌ 不采用 |

**规模判断**：50 门店 × 每店年均几十单 ≈ 数千文档/年，sqlite-vec 或纯 Python 均绰绰有余。

### 3.2 Embedding 模型

| 方案 | 说明 | 结论 |
|---|---|---|
| **OpenAI-compatible `/embeddings` 接口** | 复用现有 `LLM_BASE_URL` 同款供应商机制（DeepSeek/SiliconFlow/通义等均兼容），中文可用 bge 系列或 text-embedding-3-small | ✅ **推荐**：与现有架构零摩擦，新增 `EMBEDDING_*` 环境变量即可 |
| 本地 sentence-transformers（bge-small-zh-v1.5） | 离线、免费；但引入 torch 依赖（数百 MB），与轻量部署原则冲突 | 备选（完全离线场景） |

新增环境变量（沿用 config.py 惯例，密钥只从环境注入）：

```bash
EMBEDDING_BASE_URL=https://api.siliconflow.cn/v1   # 任意 OpenAI 兼容端点
EMBEDDING_API_KEY=sk-xxx
EMBEDDING_MODEL=BAAI/bge-m3                        # 或 text-embedding-3-small 等
QA_ENABLED=true
QA_TOP_K=4                 # 检索条数
QA_MIN_SCORE=0.35          # 低于阈值 → 拒答转人工
QA_GROUP_WHITELIST=        # 空=全部群；灰度期可只开测试群
```

### 3.3 检索策略：混合检索（第一期不做 rerank）

```
问题 → ① FTS5 关键词检索（SQLite 内建，BM25 打分，适合设备名/故障词精确命中）
      → ② 向量语义检索（vec0 KNN，适合口语化描述）
      → ③ RRF（Reciprocal Rank Fusion）融合两路排名
      → top-K 文档块
```

- 中文分词：FTS5 默认按 unicode61 切不了中文词，用 **trigram tokenizer**（SQLite 3.34+ 内建）即可支持中文子串匹配，无需引入 jieba。
- rerank（bge-reranker / LLM 打分）留作二期优化：语料小 + 提示词约束下，首期收益有限。

### 3.4 生成模型

复用 `semantics/model_client.OpenAICompatibleModelClient`，新增 `complete_text()` 方法（现有 `complete_json` 面向结构化输出，QA 需要受约束的自由文本）。仍单次调用、不内部重试、超时 60s，由 Inbox Worker 统一重试——与既有约束一致。

### 3.5 不引入 LangChain / LlamaIndex

理由：
1. 需要的能力只有「嵌入 + 余弦/FTS 检索 + 拼提示词」，合计约 300 行代码，与仓库「确定性规则内核、轻依赖」风格一致；
2. 现有 model_client 已封装供应商无关调用；框架反而带来版本与抽象成本；
3. 评测要接入现有 `semantics/run_eval.py` 体系，自研模块更顺。

---

## 四、系统设计

### 4.1 总体架构（新增部分加粗）

```
钉钉群消息
  → event_listener → event_normalizer → inbox
  → Inbox Worker → pipeline._handle
       ├─ 语义分类（协议新增 qa.answer 意图）
       │     ├─ chat.ignore        → 忽略（现状不变）
       │     ├─ ticket.*           → 既有工单链路（不变）
       │     └─ **qa.answer**      → **新链路**：
       │           ① **kb.retriever 混合检索**（FTS5 + vec0 → RRF）
       │           ② 最高分 < QA_MIN_SCORE → 「未找到资料」+ @工程师（兜底）
       │           ③ **kb.generator 生成**（检索片段 + 问题 → 答案 + 引用）
       │           ④ **notifier.send_group_now** 回群（带引用来源）
       │           ⑤ **审计落库 semantic_decisions**（intent=qa.answer）
```

### 4.2 新模块布局

```
qa/
  __init__.py
  kb_store.py        # kb_documents 建表/读写/content_hash 变更检测
  kb_builder.py      # 语料构建：工单→案例文档模板；文档→标题切块；FAQ→问答对
  embeddings.py      # /embeddings 客户端（批量、缓存、降级：无key时禁用QA意图）
  retriever.py       # 混合检索：FTS5(trigram) + sqlite-vec KNN + RRF 融合
  generator.py       # 提示词组装 + complete_text + 答案后校验（引用存在性）
scripts/
  build_kb.py        # 全量/增量重建知识库（--rebuild / --source ticket|doc|faq）
tests/
  test_kb_builder.py     # 工单→文档模板拼装（fixtures 假工单）
  test_kb_retriever.py   # 混合检索 + RRF + 阈值（本地静态向量，不调API）
  test_qa_generator.py   # 提示词组装 + 引用校验 + 拒答分支（mock 模型）
  test_qa_pipeline.py    # pipeline 集成：qa.answer 全链路（mock 检索/生成）
```

### 4.3 协议扩展（`qa.answer` 动作）

业务源（`protocol_compiler` 的 `_EXTRA_ACTIONS`）新增：

```json
{
  "intent_id": "qa.answer",
  "display_name": "知识问答",
  "explicit_keywords": ["#问"],
  "semantic_enabled": true,
  "allowed_roles": ["MANAGER", "ENGINEER", "LEADER", "OTHER"],
  "allowed_ticket_states": [],
  "required_fields": [],
  "optional_fields": ["question"],
  "target_ticket_policy": "NONE",
  "risk_level": "LOW",
  "confirmation_policy": {"EXPLICIT_KEYWORD": "NOT_REQUIRED", "SEMANTIC_MODEL": "NOT_REQUIRED"},
  "executor": "qa_answer",
  "field_definitions": {}
}
```

- `explicit_keywords: ["#问"]` 提供关键词快路径（模型不可用时仍可触发问答，与既有降级哲学一致）；
- `allowed_ticket_states: []` + `target_ticket_policy: NONE`：问答不绑定工单、无状态约束；
- 分类器提示词新增规则（`classifier._build_payload` 的规则区）：
  - 「咨询操作流程、维修经验、业务规则、SLA 政策的问题 → qa.answer」
  - 「疑问句但描述了具体故障、要求修理 → 仍是 ticket.create（既有规则保留）」
  - 正例：「空调不制冷一般什么原因」「工单超时了会怎么样」「#问 报修错了怎么撤销」
  - 反例：「空调不制冷了帮我修一下」（→ticket.create）、「你在吗」（→chat.ignore）

### 4.4 生成提示词（约束幻觉的核心）

```text
你是门店报修群的维修/业务咨询助手。只根据下方【参考资料】回答问题，
不得使用资料之外的知识；资料不足以回答时明确说"知识库中没有找到相关资料"。

要求：
1. 先给结论，再给依据；维修类问题给出「可能原因 + 建议处理方式」
2. 回答末尾用【来源】列出引用的资料编号（如 工单W2024-0131、使用须知§2）
3. 若资料是历史工单，注明"基于历史维修案例，仅供参考"
4. 不执行任何工单操作；涉及建单/撤单操作指引时只描述步骤，不代为执行
5. 60~200字，口语化，适合群聊阅读

【参考资料】
[1] (工单W2024-0131, 相关度0.82) 冷柜不制冷……
[2] (使用须知§2, 相关度0.71) ……

【用户问题】
{question}
```

### 4.5 回群消息格式

```
💡 自动答复（知识库检索）：
冷柜不制冷常见原因是制冷剂泄漏或冷凝器积灰，建议先断电检查冷凝器…
——基于历史维修案例，仅供参考
【来源】工单 W2024-0131 · 使用须知§5
```

拒答兜底（自动化安全阀——目标是减少人工，但知识边界外仍需可逃逸）：

```
🤖 知识库中没有找到相关资料，已通知工程师。@聂宇清
```

### 4.6 防护措施（对齐既有安全原则）

| 风险 | 措施 |
|---|---|
| 幻觉答案 | 提示词强约束 + 生成后校验：答案中出现的工单号/文档名必须在检索结果内，否则降级拒答 |
| 误把报修当咨询 | 分类器规则 + 评测集正反例；QA 置信度低时按 chat.ignore 处理（宁可不答，不错答） |
| 自激励循环 | 系统账号回流已过滤（现状）；另用 notifier 既有 `send_deduped_group` 对相同问题 10 分钟去重 |
| 用户提示注入 | 用户消息仍作为不可信数据字段（§10.4 既有做法）；【参考资料】由系统拼装 |
| 知识过时 | content_hash 变更检测增量重嵌入；工单重开→终态变化时重建该案例 |
| 问答打扰群 | 默认只回复 @机器人 或 #问 的消息（第一期）；自然语言问句灰度后再开 |

### 4.7 评测方案（复用现有体系）

1. `tests/fixtures/` 新增 `qa_cases.json`：带标注的问题→期望命中的 doc_id / 期望拒答；
2. `semantics/run_eval.py` 扩展 QA 评测：意图识别准确率（咨询 vs 报修 vs 闲聊）+ 检索命中率（top-K 含标注 doc）；
3. 上线走既有三段式：`SHADOW`（只记录不回答）→ `ASSISTED`（白名单群回答）→ `PRODUCTION`；
4. 审计：`semantic_decisions` 记录 intent=qa.answer、检索到的 doc 列表与分数、答案摘要，支持事后复盘。

### 4.8 知识库更新机制

- **增量**：`workers/scheduler` 新增定时任务（默认 30 分钟）：扫描 content_hash 变化的文档 + 新终态工单 → 重嵌入；
- **全量**：`python scripts/build_kb.py --rebuild`（模型/维度切换后必须全量重建）；
- 嵌入写入走批量接口，失败退避重试，不阻塞调度器。

---

## 五、实施计划（TDD，7 个可独立验收的任务）

| # | 任务 | 交付物 | 验收标准 |
|---|---|---|---|
| 1 | kb_store + 表结构 | `qa/kb_store.py` + 迁移 | 建表幂等；content_hash 变更检测单测过 |
| 2 | kb_builder 语料构建 | `qa/kb_builder.py` + `scripts/build_kb.py` | 假工单 fixtures → 案例文档模板正确；CANCELLED 不入库 |
| 3 | embeddings 客户端 | `qa/embeddings.py` | 批量嵌入 + 失败异常 + 无 key 时 is_configured=False |
| 4 | retriever 混合检索 | `qa/retriever.py` | 静态向量单测：FTS 命中、向量命中、RRF 融合、阈值拒答 |
| 5 | 协议 + 分类器扩展 | protocol_compiler 新动作 + 提示词规则 + 用例 | 意图评测：咨询/报修/闲聊混淆矩阵达标（≥95%） |
| 6 | generator + pipeline 接线 | `qa/generator.py` + pipeline 分支 + notifier 格式 | 集成测试：mock 检索/生成全链路；引用校验；拒答分支 |
| 7 | 评测 + 灰度上线 | qa_cases.json + SHADOW 报告 | SHADOW 跑一周，误触发率 <2% 后开白名单 |

依赖变化：`requirements.txt` 新增 `sqlite-vec`、`numpy`（余弦计算）；其余全部复用。

---

## 六、已确认决策（2026-08-20 业务确认）

1. **触发范围**：✅ 第一期只响应 `#问` 显式关键词 / @机器人，零误触发风险；稳定一个周期后再评估放开自然语言咨询。
2. **拒答兜底**：✅ 检索不到时 @固定工程师（当前为聂宇清，取群配置中该群 engineer_ids），保证问题不落空。
3. **答案不做人工复核**：答案直接发群（复核会重新引入人工，与自动化目标矛盾）；质量靠评测 + 审计复盘保障。
4. **语料冷启动**：实施任务 2 时统计实际终态工单量；不足 200 单时维修经验类答案标注"案例较少，仅供参考"。

## 七、开放问题（实施中再议）

- 自然语言咨询放开的时机与准确率门槛（SHADOW 观察数据决定）。
- FAQ 文件（data/faq.md）由谁维护、更新频率。
