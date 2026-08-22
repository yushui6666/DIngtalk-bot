"""RAG 闭环顾问模块（方案 v2.0）。

子模块：
- kb_store     知识文档存储（文档表 + FTS5 trigram 索引 + 向量 BLOB）
- kb_builder   语料构建（终态工单→案例文档；业务文档→标题切块）
- embeddings   OpenAI 兼容 /embeddings 客户端
- retriever    混合检索（FTS5 关键词 + 向量语义 → RRF 融合）
- advisor      建单后建议生成与群通知
- feedback     反馈（解决了/未解决）与升级
"""

from qa.kb_store import KBStore  # noqa: F401
