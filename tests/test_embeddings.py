"""embeddings 客户端单测：批量、降级、鉴权（全部 mock HTTP，不调真实 API）。"""

from __future__ import annotations

import json

import httpx
import pytest

from qa.embeddings import EmbeddingClient, EmbeddingNotConfiguredError


def _ok_response(vecs: list[list[float]]) -> httpx.Response:
    data = {
        "object": "list",
        "data": [
            {"object": "embedding", "index": i, "embedding": v}
            for i, v in enumerate(vecs)
        ],
        "model": "test-embed",
        "usage": {"prompt_tokens": 1, "total_tokens": 1},
    }
    return httpx.Response(200, json=data, request=httpx.Request("POST", "http://t/embeddings"))


class TestConfiguration:
    def test_not_configured_without_key(self, monkeypatch):
        monkeypatch.delenv("EMBEDDING_API_KEY", raising=False)
        client = EmbeddingClient()
        assert client.is_configured is False
        with pytest.raises(EmbeddingNotConfiguredError):
            import asyncio
            asyncio.get_event_loop().run_until_complete(
                client.embed(["你好"]))

    def test_configured_reads_env(self, monkeypatch):
        monkeypatch.setenv("EMBEDDING_API_KEY", "sk-test")
        client = EmbeddingClient()
        assert client.is_configured is True
        assert client.model


class TestEmbed:
    @pytest.mark.asyncio
    async def test_batch_embed_order_preserved(self, monkeypatch):
        monkeypatch.setenv("EMBEDDING_API_KEY", "sk-test")
        client = EmbeddingClient()
        captured: dict = {}

        async def fake_post(self, url, **kw):
            captured.update(kw)
            return _ok_response([[1.0, 0.0], [0.0, 1.0]])

        monkeypatch.setattr(httpx.AsyncClient, "post", fake_post)
        vecs = await client.embed(["第一条", "第二条"])
        assert len(vecs) == 2
        assert vecs[0] == [1.0, 0.0] and vecs[1] == [0.0, 1.0]
        raw = captured.get("json")
        body = raw if isinstance(raw, dict) else json.loads(raw or captured.get("content"))
        assert body["input"] == ["第一条", "第二条"]

    @pytest.mark.asyncio
    async def test_single_string_wrapped(self, monkeypatch):
        monkeypatch.setenv("EMBEDDING_API_KEY", "sk-test")
        client = EmbeddingClient()

        async def fake_post(self, url, **kw):
            return _ok_response([[0.5, 0.5]])

        monkeypatch.setattr(httpx.AsyncClient, "post", fake_post)
        vecs = await client.embed("单条")
        assert vecs == [[0.5, 0.5]]

    @pytest.mark.asyncio
    async def test_http_error_raises(self, monkeypatch):
        monkeypatch.setenv("EMBEDDING_API_KEY", "sk-test")
        client = EmbeddingClient()

        async def fake_post(self, url, **kw):
            return httpx.Response(500, text="boom",
                                  request=httpx.Request("POST", "http://t/embeddings"))

        monkeypatch.setattr(httpx.AsyncClient, "post", fake_post)
        with pytest.raises(RuntimeError):
            await client.embed(["x"])

    @pytest.mark.asyncio
    async def test_dimension_property(self, monkeypatch):
        monkeypatch.setenv("EMBEDDING_API_KEY", "sk-test")
        client = EmbeddingClient()
        assert client.dimension is None
        async def fake_post(self, url, **kw):
            return _ok_response([[0.1] * 8])
        monkeypatch.setattr(httpx.AsyncClient, "post", fake_post)
        await client.embed(["校准"])
        assert client.dimension == 8
