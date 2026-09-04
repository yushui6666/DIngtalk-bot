# 管理后台

直接操作 `data/tickets.db` 的可视化后台。

## 启动

```bash
# 默认端口 8899，密码 admin123
python3 dingtalk_script/admin/server.py --port 8899

# 自定义密码
ADMIN_PASSWORD=你的密码 python3 dingtalk_script/admin/server.py --port 8899

# 指定数据库
DB_PATH=/path/to/tickets.db python3 dingtalk_script/admin/server.py
```

浏览器打开: **http://127.0.0.1:8899**

## 功能

- **仪表盘**: 工单/消息/订单统计 + 最近工单 + 快捷入口
- **🎫 工单管理（最常用）**: 左侧顶部「＋ 新建工单」/「工单管理」
  - 新建：选门店 → 填主题/位置/问题描述 → 选时效（1天/3天/7天/待商榷）→ 创建；
    编号自动生成（店名-主题-时效-序号），走 `TicketRepository.create_ticket`，与群内建单规则完全一致
  - 编辑状态：卡片点「编辑状态」→ 六选一（进行中/待确认/待商榷/已完成/已取消/已停修）；
    副作用与主系统对齐：终态写 `closed_at` 与对应留痕（`admin-manual`）+ 关责任周期 + 清用户上下文，从终态切回进行中按重开处理（`closed_at` 清空、`reopen_count`+1、清 SLA 去重）
  - 删除：卡片点「删除」→ 先展示关联数据（消息/判断/方案/订单等计数）→ 确认后级联删除并整库备份到 `data/tickets.db.bak_admin_del_<id>_<时间>`
  - 卡片列表支持编号/门店/主题搜索 + 状态筛选
- **22 张表直接操作**: 左侧按业务分组，支持搜索、排序、分页
  - 双击单元格直接改，自动备份
  - 点击表头排序
  - 新增行（自增主键自动跳过）
  - 删除行、导出 CSV
- **SQL 控制台**: 任意 SQL，写操作自动备份 `data/tickets.db.bak_admin_*`
- **一键备份**: 顶部“备份数据库”

## 表分组

- 核心业务: tickets, groups, ticket_special_cases
- 消息链路: inbox_messages, messages, message_ticket_links, message_attachments, semantic_decisions
- 维修流程: diagnosis_versions, repair_method_versions, timeout_cycles, responsibility_cycles
- 订单/物流: order_monitor, taobao_orders, delivery_confirmations
- 系统/队列: pending_actions, action_executions, ticket_contexts, notification_deliveries, processed_events, schema_migrations

## 安全

- 写操作前自动 `shutil.copy2` 备份
- 所有 SQL/更新走同一 DB 文件，WAL 模式
- 登录密码存 `ADMIN_PASSWORD` 环境变量，默认 `admin123`
