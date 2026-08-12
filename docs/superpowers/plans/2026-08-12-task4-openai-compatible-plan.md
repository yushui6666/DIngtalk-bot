# Task 4 OpenAI-Compatible Model Integration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Complete the OpenAI-compatible semantic classifier with 60-second default timeout, single-call response-format compatibility, strict local validation, and a real-model evaluator CLI.

**Architecture:** Keep `OpenAICompatibleModelClient` responsible only for one HTTP request and response extraction. Keep `SemanticClassifier` responsible for protocol-constrained prompt construction and safe normalization. Extend the existing evaluator CLI to select keyword baseline or live-model prediction without duplicating metric logic.

**Tech Stack:** Python 3.14, `httpx`, `jsonschema`, `pytest`, `pytest-asyncio`, OpenAI-compatible Chat Completions.

---

### Task 1: Response Format Configuration

**Files:**
- Modify: `config.py`
- Modify: `semantics/model_client.py`
- Test: `tests/test_model_contract.py`

- [x] Add failing tests for the 60-second default timeout and `json_schema`, `json_object`, and `auto` request bodies.
- [x] Run the focused tests and confirm failures identify missing configuration behavior.
- [x] Add `LLM_RESPONSE_FORMAT`, validate allowed values, and resolve `auto` from the base URL before sending the single request.
- [x] Run the focused tests and confirm all format modes pass without retries.

### Task 2: Model Response Safety

**Files:**
- Modify: `semantics/model_client.py`
- Modify: `semantics/classifier.py`
- Test: `tests/test_model_contract.py`

- [x] Add failing tests for non-object JSON, malformed JSON content, unknown intent, hallucinated fields, unsafe target tickets, and secret-free errors/log metadata.
- [x] Run the focused tests and confirm the unsafe outputs fail safely.
- [x] Enforce object responses, protocol intent allowlists, action field allowlists, enum filtering, candidate-only target routing, confidence bounds, and safe fallback decisions.
- [x] Run the focused tests and confirm one model call per classification.

### Task 3: Prompt Context Contract

**Files:**
- Modify: `semantics/classifier.py`
- Test: `tests/test_model_contract.py`

- [x] Add failing tests asserting the payload contains protocol action constraints, candidate snapshots, sender role, and restricted pending-action context.
- [x] Run the focused tests and confirm missing prompt contract details.
- [x] Add only the required protocol, candidate, and pending-action fields to the prompt and fixed output schema.
- [x] Run the focused tests and confirm no unrelated secrets or configuration enter the payload.

### Task 4: Live Evaluator CLI

**Files:**
- Modify: `semantics/evaluator.py`
- Modify: `semantics/run_eval.py`
- Test: `tests/test_model_contract.py`
- Test: `tests/test_semantic_evaluator.py`

- [x] Add failing CLI tests for `--live-model --dataset PATH`, `.env` loading, and metric output.
- [x] Run the CLI tests and confirm the option is not yet supported.
- [x] Reuse the model client and classifier to generate `EvalPrediction` objects and feed the existing `evaluate()` function.
- [x] Run keyword and live-model CLI tests and confirm both output the same metric schema.

### Task 5: Verification and Single Commit

**Files:**
- Modify: `docs/superpowers/specs/2026-08-12-task4-openai-compatible-design.md`
- Create: `docs/superpowers/plans/2026-08-12-task4-openai-compatible-plan.md`

- [x] Run `python -m pytest tests/test_model_contract.py -q`.
- [x] Run `python -m pytest tests -q`.
- [x] Run the configured real-model blind evaluation with `python -m semantics.evaluator --live-model --dataset tests/fixtures/semantic_cases.blind.json`.
- [x] Review `git diff --check`, ensure no API key is tracked, and inspect the final diff.
- [x] Commit all Task 4 design, implementation, tests, and documentation once with message `完成 Task 4 云端语义模型接入`.
