"""Tests for the webhook Retry + Dead-Letter Queue."""

import asyncio
import json
from contextlib import suppress

import pytest
from sqlalchemy import select

import app.routers.ingestion as ingestion_router
from app.database import AsyncSessionLocal
from app.main import app
from app.models import FailedWebhook, Product
from app.services import dlq
from app.services.ingestion import upsert_product as real_upsert

SOURCE = "retry-test"

PAYLOAD = {
    "external_id": "RETRY-001",
    "name": "Retry Widget",
    "price": "10.00",
    "currency": "USD",
    "stock": 1,
}


async def post_webhook(client, payload):
    return await client.post(
        f"/ingest/webhook/{SOURCE}",
        content=json.dumps(payload),
        headers={"Content-Type": "application/json"},
    )


async def failed_rows() -> list:
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(FailedWebhook).where(FailedWebhook.source == SOURCE)
        )
        return list(result.scalars())


def make_failing_upsert(fail_for: str):
    async def failing_upsert(session, source, payload):
        if payload.external_id == fail_for:
            raise RuntimeError("simulated database outage")
        return await real_upsert(session, source, payload)

    return failing_upsert


async def test_failed_webhook_is_queued_then_completed(client, monkeypatch) -> None:
    # Failing upsert: request must degrade to 202 and persist a pending row.
    monkeypatch.setattr(
        ingestion_router, "upsert_product", make_failing_upsert("RETRY-001")
    )
    resp = await post_webhook(client, PAYLOAD)
    assert resp.status_code == 202
    assert resp.json() == {"status": "queued_for_retry"}

    rows = await failed_rows()
    assert len(rows) == 1
    assert rows[0].status == "pending"
    assert rows[0].attempt_count == 0
    assert rows[0].payload["external_id"] == "RETRY-001"

    # Restore the real upsert and run one retry pass manually.
    monkeypatch.setattr(ingestion_router, "upsert_product", real_upsert)
    stats = await dlq.run_pending_retries()
    assert stats == {
        "attempted": 1,
        "completed": 1,
        "failed": 0,
        "dead_lettered": 0,
        "skipped": 0,
    }

    rows = await failed_rows()
    assert rows[0].status == "completed"
    assert rows[0].attempt_count == 0  # only incremented on failure

    async with AsyncSessionLocal() as session:
        product = await session.scalar(
            select(Product).where(
                Product.source == SOURCE, Product.external_id == "RETRY-001"
            )
        )
    assert product is not None
    assert product.name == "Retry Widget"


async def test_row_moves_to_dead_letter_after_five_attempts(client, monkeypatch) -> None:
    monkeypatch.setattr(
        ingestion_router, "upsert_product", make_failing_upsert("RETRY-002")
    )
    monkeypatch.setattr(dlq, "upsert_product", make_failing_upsert("RETRY-002"))

    resp = await post_webhook(client, {**PAYLOAD, "external_id": "RETRY-002"})
    assert resp.status_code == 202

    outcomes = [await dlq.run_pending_retries() for _ in range(dlq.MAX_ATTEMPTS)]
    assert [s["failed"] for s in outcomes] == [1, 1, 1, 1, 0]
    assert outcomes[-1]["dead_lettered"] == 1

    rows = await failed_rows()
    assert rows[0].status == "dead_letter"
    assert rows[0].attempt_count == 5
    assert "attempt 5" in rows[0].error_message

    # dead_letter rows are never retried again
    assert (await dlq.run_pending_retries())["attempted"] == 0


async def test_completed_rows_are_not_retried() -> None:
    async with AsyncSessionLocal() as session:
        session.add(
            FailedWebhook(
                source=SOURCE,
                payload={},
                error_message="",
                attempt_count=0,
                status="completed",
            )
        )
        await session.commit()
        row_id = (
            (await session.execute(select(FailedWebhook.id))).scalars().first()
        )

    assert await dlq._retry_one(row_id) == "skipped"


async def test_retry_loop_survives_errors_and_runs_passes(monkeypatch) -> None:
    calls = {"n": 0}

    async def flaky_pass():
        calls["n"] += 1
        if calls["n"] == 2:
            raise RuntimeError("transient failure")
        return {"attempted": 1, "completed": 1, "failed": 0, "dead_lettered": 0, "skipped": 0}

    monkeypatch.setattr(dlq, "run_pending_retries", flaky_pass)
    monkeypatch.setattr(dlq, "RETRY_INTERVAL_SECONDS", 0.01)

    task = dlq.start_retry_worker()
    await asyncio.sleep(0.1)
    dlq.stop_retry_worker()
    with suppress(asyncio.CancelledError):
        await task

    assert calls["n"] >= 3  # loop kept going past the raised error


def test_stop_worker_without_start_is_noop(monkeypatch) -> None:
    monkeypatch.setattr(dlq, "_worker_task", None)
    dlq.stop_retry_worker()  # must not raise


async def test_lifespan_starts_and_stops_worker() -> None:
    async with app.router.lifespan_context(app):
        assert dlq._worker_task is not None
        assert not dlq._worker_task.done()
    assert dlq._worker_task.cancelled()