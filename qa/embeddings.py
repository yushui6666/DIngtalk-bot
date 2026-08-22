"""OpenAI 兼容 /embeddings 客户端（方案 v2.0 任务 3）。

约束与 semantics/model_client 一致：
- 单次 HTTP 调用（httpx AsyncClient），超时可配；
- 不内部重试（调用方统一管理重试/退避）；
- API Key 只从构造参数或 EMBEDDING_API_KEY 环境变量读取，
  绝不写入日志或异常消息；
- 未配置 Key 时 is_configured=False，embed() 抛 EmbeddingNotConfiguredError
  （上层据此跳过嵌入，知识库链路静默降级）。

环境变量：
- EMBEDDING_BASE_URL   默认复用 LLM_BASE_URL
- EMBEDDING_API_KEY    必填才启用
- EMBEDDING_MODEL      如 BAAI/bge-m3、text-embedding-3-small
- EMBEDDING_TIMEOUT_SECONDS  默认 60
- EMBEDDING_BATCH_SIZE 默认 32
"""

from __future__ import annotations

import os
from typing import Union

import httpx

from logger import get_logger

logger = get_logger(__name__)


class EmbeddingNotConfiguredError(RuntimeError):
    """未配置 API Key 即调用嵌入。"""


class EmbeddingClient:
    """轻量异步 /embeddings 客户端。"""

    def __init__(
        self,
        *,
        base_url: str | None = None,
        api_key: str | None = None,
        model: str | None = None,
        timeout_seconds: float | None = None,
        batch_size: int | None = None,
    ) -> None:
        self.base_url = (
            base_url
            or os.environ.get("EMBEDDING_BASE_URL")
            or os.environ.get("LLM_BASE_URL", "https://api.openai.com/v1")
        ).rstrip("/")
        self._api_key = api_key or os.environ.get("EMBEDDING_API_KEY", "")
        self.model = model or os.environ.get("EMBEDDING_MODEL", "text-embedding-3-small")
        self.timeout_seconds = timeout_seconds if timeout_seconds is not None else float(
            os.environ.get("EMBEDDING_TIMEOUT_SECONDS", "60"))
        self.batch_size = batch_size or int(os.environ.get("EMBEDDING_BATCH_SIZE", "32"))
        self._dimension: int | None = None

    @property
    def is_configured(self) -> bool:
        return bool(self._api_key)

    @property
    def dimension(self) -> int | None:
        """首次成功嵌入后可知；此前为 None。"""
        return self._dimension

    async def embed(
        self, texts: Union[str, list[str]]
    ) -> list[list[float]]:
        """嵌入一条或多条文本，顺序与输入一致（不内部重试）。"""
        if isinstance(texts, str):
            texts = [texts]
        if not texts:
            return []
        if not self.is_configured:
            raise EmbeddingNotConfiguredError("嵌入服务未配置 API Key")

        results: list[list[float]] = []
        for start in range(0, len(texts), self.batch_size):
            batch = texts[start:start + self.batch_size]
            results.extend(await self._embed_batch(batch))
        return results

    async def _embed_batch(self, batch: list[str]) -> list[list[float]]:
        payload = {"model": self.model, "input": batch}
        headers = {"Authorization": f"Bearer {self._api_key}"}
        async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
            resp = await client.post(
                f"{self.base_url}/embeddings", json=payload, headers=headers
            )
        if resp.status_code != 200:
            # 不回显响应体中可能存在的敏感信息，只带状态码
            raise RuntimeError(f"嵌入服务返回 HTTP {resp.status_code}")
        data = resp.json()
        try:
            items = sorted(data["data"], key=lambda d: d.get("index", 0))
            vectors = [[float(x) for x in item["embedding"]] for item in items]
        except (KeyError, TypeError, ValueError) as exc:
            raise RuntimeError("嵌入响应结构异常") from exc
        if len(vectors) != len(batch):
            raise RuntimeError(
                f"嵌入返回数量不符 input={len(batch)} output={len(vectors)}")
        if self._dimension is None and vectors:
            self._dimension = len(vectors[0])
        return vectors
