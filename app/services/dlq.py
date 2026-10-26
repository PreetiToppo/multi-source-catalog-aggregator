"""Retry + Dead-Letter Queue for failed webhook ingestion.

Failed payloads are saved to failed_webhooks with status="pending"; a
background loop retries them every RETRY_INTERVAL_SECONDS. After
MAX_ATTEMPTS failed retries a row is moved to status="dead_letter" (DLQ).
"""

import asyncio
import logging

from sqlalchemy import select

from app.database import AsyncSessionLocal
from app.models import FailedWebhook
from app.schemas import RestProductPayload
from app.services.ingestion import upsert_product

logger = logging.getLogger("app.dlq")

RETRY_INTERVAL_SECONDS = 30
MAX_ATTEMPTS = 5

_worker_task: asyncio.Task | None = None


async def save_failed_webhook(source: str, payload: dict, error_message: str) -> None:
    """Persist a payload that could not be ingested (own session, so a broken
    request session can't block it)."""
    async with AsyncSessionLocal() as session:
        session.add(
            FailedWebhook(
                source=source,
                payload=payload,
                error_message=error_message[:2000],
                attempt_count=0,
                status="pending",
            )
        )
        await session.commit()


async def _retry_one(row_id: int) -> str:
    """Retry a single failed row; returns completed/failed/dead_lettered/skipped."""
    async with AsyncSessionLocal() as session:
        row = await session.get(FailedWebhook, row_id)
        if row is None or row.status != "pending":
            return "skipped"
        try:
            payload = RestProductPayload.model_validate(row.payload)
            await upsert_product(session, row.source, payload)  # commits
        except Exception as e:  # noqa: BLE001
            # Roll back before recording the failure: an aborted transaction
            # would otherwise poison the bookkeeping update as well.
            await session.rollback()
            try:
                row = await session.get(FailedWebhook, row_id)
                row.attempt_count += 1
                row.error_message = f"attempt {row.attempt_count}: {e}"[:2000]
                if row.attempt_count >= MAX_ATTEMPTS:
                    row.status = "dead_letter"
                    await session.commit()
                    logger.warning(
                        "failed_webhooks id=%s moved to dead_letter after %s attempts",
                        row_id,
                        row.attempt_count,
                    )
                    return "dead_lettered"
                await session.commit()
                return "failed"
            except Exception:  # noqa: BLE001
                logger.exception("could not record failed retry for id=%s", row_id)
                return "failed"
        row.status = "completed"
        await session.commit()
        return "completed"


async def run_pending_retries() -> dict:
    """One retry pass over pending rows with attempt_count < MAX_ATTEMPTS."""
    stats = {
        "attempted": 0,
        "completed": 0,
        "failed": 0,
        "dead_lettered": 0,
        "skipped": 0,
    }
    async with AsyncSessionLocal() as session:
        row_ids = (
            (
                await session.execute(
                    select(FailedWebhook.id).where(
                        FailedWebhook.status == "pending",
                        FailedWebhook.attempt_count < MAX_ATTEMPTS,
                    )
                )
            )
            .scalars()
            .all()
        )
    for row_id in row_ids:
        stats["attempted"] += 1
        stats[await _retry_one(row_id)] += 1
    return stats


async def retry_loop() -> None:
    """Background task: run a retry pass every RETRY_INTERVAL_SECONDS."""
    while True:
        try:
            stats = await run_pending_retries()
            if stats["attempted"]:
                logger.info("retry pass: %s", stats)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - must not kill the worker loop
            logger.exception("retry pass failed")
        await asyncio.sleep(RETRY_INTERVAL_SECONDS)


def start_retry_worker() -> asyncio.Task:
    global _worker_task
    _worker_task = asyncio.create_task(retry_loop())
    return _worker_task


def stop_retry_worker() -> None:
    if _worker_task is not None:
        _worker_task.cancel()