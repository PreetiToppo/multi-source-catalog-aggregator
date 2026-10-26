"""Tests for POST /ingest/webhook/{source_name} and signature verification."""

import hashlib
import hmac
import json
import logging
import os

from sqlalchemy import select

import app.routers.ingestion as ingestion_router
from app.database import AsyncSessionLocal
from app.models import FailedWebhook, Product
from app.services import dlq
from app.services.ingestion import (
    upsert_product as real_upsert,
    verify_webhook_signature,
)

SECRET = os.getenv("WEBHOOK_SECRET", "test-secret-123")

PAYLOAD = {
    "external_id": "WH-1",
    "name": "Webhook Widget",
    "price": "33.00",
    "currency": "USD",
    "stock": 9,
}


def sign(body: bytes, secret: str = SECRET) -> str:
    return "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()


async def post_webhook(client, source, payload, headers=None):
    return await client.post(
        f"/ingest/webhook/{source}",
        content=json.dumps(payload),
        headers={"Content-Type": "application/json", **(headers or {})},
    )


# --- endpoint behaviour -----------------------------------------------------


async def test_webhook_inserts_with_path_source(client) -> None:
    resp = await post_webhook(client, "shopify", PAYLOAD, {"X-Signature-256": sign(b"")})
    assert resp.status_code == 200
    assert resp.json()["status"] == "accepted"

    async with AsyncSessionLocal() as session:
        row = await session.scalar(select(Product).where(Product.external_id == "WH-1"))
    assert row.source == "shopify"


async def test_webhook_updates_existing(client) -> None:
    body = json.dumps(PAYLOAD).encode()
    headers = {"X-Signature-256": sign(body)}
    await client.post(
        "/ingest/webhook/shopify",
        content=body,
        headers={"Content-Type": "application/json", **headers},
    )
    resp = await post_webhook(client, "shopify", {**PAYLOAD, "price": "40.00"})
    assert resp.json()["status"] == "updated"

    async with AsyncSessionLocal() as session:
        row = await session.scalar(select(Product).where(Product.external_id == "WH-1"))
    assert float(row.price) == 40.00


async def test_webhook_bad_signature_still_processes(client, caplog) -> None:
    with caplog.at_level(logging.WARNING, logger="app.ingestion"):
        resp = await post_webhook(
            client, "shopify", PAYLOAD, {"X-Signature-256": "sha256=deadbeef"}
        )
    assert resp.status_code == 200
    assert resp.json()["status"] == "accepted"
    assert any("signature mismatch" in r.message for r in caplog.records)


async def test_webhook_missing_signature_header_warns(client, caplog) -> None:
    with caplog.at_level(logging.WARNING, logger="app.ingestion"):
        resp = await post_webhook(client, "shopify", PAYLOAD)
    assert resp.status_code == 200
    assert any("missing X-Signature-256" in r.message for r in caplog.records)


async def test_webhook_valid_signature_processes_quietly(client, caplog) -> None:
    body = json.dumps(PAYLOAD).encode()
    with caplog.at_level(logging.WARNING, logger="app.ingestion"):
        resp = await post_webhook(
            client, "shopify", PAYLOAD, {"X-Signature-256": sign(body)}
        )
    assert resp.status_code == 200
    assert not any("webhook" in r.message for r in caplog.records)


async def test_webhook_invalid_json_422(client) -> None:
    resp = await client.post(
        "/ingest/webhook/shopify",
        content=b"{not json",
        headers={"Content-Type": "application/json"},
    )
    assert resp.status_code == 422


async def test_webhook_missing_field_422(client) -> None:
    resp = await post_webhook(client, "shopify", {"external_id": "X"})
    assert resp.status_code == 422


async def test_webhook_source_name_too_long_422(client) -> None:
    resp = await post_webhook(client, "s" * 65, PAYLOAD)
    assert resp.status_code == 422


async def test_webhook_upsert_failure_queues_202(client, monkeypatch) -> None:
    async def failing_upsert(session, source, payload):
        raise RuntimeError("simulated outage")

    monkeypatch.setattr(ingestion_router, "upsert_product", failing_upsert)
    resp = await post_webhook(client, "shopify", PAYLOAD)
    assert resp.status_code == 202
    assert resp.json() == {"status": "queued_for_retry"}

    async with AsyncSessionLocal() as session:
        rows = (await session.execute(select(FailedWebhook))).scalars().all()
    assert len(rows) == 1
    row = rows[0]
    assert row.status == "pending"
    assert row.attempt_count == 0
    assert row.source == "shopify"
    assert row.payload["external_id"] == "WH-1"
    assert "simulated outage" in row.error_message


async def test_webhook_queue_failure_returns_503(client, monkeypatch) -> None:
    async def broken_save(source, payload, error_message):
        raise RuntimeError("db fully down")

    async def failing_upsert(session, source, payload):
        raise RuntimeError("simulated outage")

    monkeypatch.setattr(ingestion_router, "upsert_product", failing_upsert)
    monkeypatch.setattr(dlq, "save_failed_webhook", broken_save)
    resp = await post_webhook(client, "shopify", PAYLOAD)
    assert resp.status_code == 503
    assert resp.json() == {"status": "queue_unavailable"}


# --- signature helper unit tests -------------------------------------------


def test_verify_signature_valid() -> None:
    body = b'{"a": 1}'
    assert verify_webhook_signature(body, sign(body)) is True


def test_verify_signature_mismatch() -> None:
    assert verify_webhook_signature(b'{"a": 1}', "sha256=bad") is False


def test_verify_signature_missing_header() -> None:
    assert verify_webhook_signature(b"{}", None) is False


def test_verify_signature_without_prefix() -> None:
    body = b"{}"
    raw = hmac.new(SECRET.encode(), body, hashlib.sha256).hexdigest()
    assert verify_webhook_signature(body, raw) is True


def test_verify_signature_no_secret(monkeypatch, caplog) -> None:
    monkeypatch.delenv("WEBHOOK_SECRET", raising=False)
    with caplog.at_level(logging.WARNING, logger="app.ingestion"):
        assert verify_webhook_signature(b"{}", "anything") is True
    assert any("WEBHOOK_SECRET not set" in r.message for r in caplog.records)