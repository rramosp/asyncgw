"""Unit tests for the policy routing engine."""

import pytest
from asyncgw.models.request import AsyncRequestEnvelope, RequestType


def test_default_routing(routing_engine):
    envelope = AsyncRequestEnvelope(
        request_id="req_1",
        request_type=RequestType.CHAT_COMPLETION,
        model="gemini-2.0-flash",
        payload={"messages": [{"role": "user", "content": "Hello"}]},
    )
    decision = routing_engine.route_request(envelope)
    assert decision.primary_backend.id == "gcp-provisioned-gemini"
    assert [b.id for b in decision.backup_backends] == ["gemini-flex", "openai-direct"]
    assert decision.requires_batch_breakdown is False


def test_batch_routing_capabilities(routing_engine):
    # Batch request targeting provisioned (supports batch)
    envelope = AsyncRequestEnvelope(
        request_id="req_batch_1",
        request_type=RequestType.BATCH,
        model="gemini-2.0-flash",
        payload={"requests": [{"custom_id": "c1", "method": "POST", "url": "/v1/chat/completions", "body": {}}]},
    )
    decision = routing_engine.route_request(envelope)
    assert decision.primary_backend.id == "gcp-provisioned-gemini"
    assert decision.requires_batch_breakdown is False

    # Batch request targeting gemini-flex (does NOT support batch)
    envelope_flex = AsyncRequestEnvelope(
        request_id="req_batch_2",
        request_type=RequestType.BATCH,
        target_backend="gemini-flex",
        model="gemini-2.0-flash",
        payload={"requests": [{"custom_id": "c1", "method": "POST", "url": "/v1/chat/completions", "body": {}}]},
    )
    decision_flex = routing_engine.route_request(envelope_flex)
    assert decision_flex.primary_backend.id == "gemini-flex"
    assert decision_flex.requires_batch_breakdown is True


def test_content_rule_token_threshold(routing_engine):
    # Long text exceeding 5000 estimated tokens (20,000 chars)
    long_text = "word " * 6000
    envelope = AsyncRequestEnvelope(
        request_id="req_large",
        request_type=RequestType.CHAT_COMPLETION,
        model="gemini-2.0-flash",
        payload={"messages": [{"role": "user", "content": long_text}]},
    )
    decision = routing_engine.route_request(envelope)
    assert decision.primary_backend.id == "gcp-provisioned-gemini"
    assert decision.strategy_id == "token_rule"


def test_content_rule_urgent_deadline(routing_engine):
    envelope = AsyncRequestEnvelope(
        request_id="req_urgent",
        request_type=RequestType.CHAT_COMPLETION,
        model="gemini-2.0-flash",
        max_wait_seconds=5,
        payload={"messages": [{"role": "user", "content": "Quick question"}]},
    )
    decision = routing_engine.route_request(envelope)
    assert decision.strategy_id == "latency_sensitive"
    assert decision.primary_backend.id == "gemini-flex"


def test_model_mapping_rule(routing_engine):
    envelope = AsyncRequestEnvelope(
        request_id="req_gpt",
        request_type=RequestType.CHAT_COMPLETION,
        model="gpt-4o",
        payload={"messages": [{"role": "user", "content": "Hello GPT"}]},
    )
    decision = routing_engine.route_request(envelope)
    assert decision.primary_backend.id == "openai-direct"
