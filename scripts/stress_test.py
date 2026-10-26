"""Stress test: push 10,000 unique products through POST /ingest/rest.

Requires the app to be running and the Docker Postgres to be up.

Usage:
    python scripts/stress_test.py [base_url]

base_url defaults to STRESS_BASE_URL or http://127.0.0.1:8000.
"""

import asyncio
import os
import sys
import time
from pathlib import Path

import httpx
from dotenv import load_dotenv
from sqlalchemy import delete, func, select

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")

from app.database import AsyncSessionLocal  # noqa: E402
from app.models import Product  # noqa: E402

TOTAL_RECORDS = 10_000
CONCURRENCY = 50
SOURCE = "rest"


def build_payload(i: int) -> dict:
    return {
        "external_id": f"stress-{i}",
        "name": f"Item {i}",
        "price": 10.99,
        "currency": "USD",
        "stock": 100,
    }


async def clear_previous_run() -> int:
    """Remove rows from an earlier run so the final count is exact."""
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            delete(Product).where(Product.source == SOURCE).returning(Product.id)
        )
        await session.commit()
        return len(result.fetchall())


async def send_all(client: httpx.AsyncClient, base_url: str) -> tuple[dict, list]:
    semaphore = asyncio.Semaphore(CONCURRENCY)
    stats = {"accepted": 0, "updated": 0, "other": 0}
    failures: list[str] = []
    done = 0

    async def send(i: int) -> None:
        nonlocal done
        async with semaphore:
            try:
                resp = await client.post(
                    f"{base_url}/ingest/rest", json=build_payload(i)
                )
            except Exception as e:  # noqa: BLE001
                failures.append(f"stress-{i}: {type(e).__name__}: {e}")
                return
            finally:
                done += 1
                if done % 2000 == 0:
                    print(f"  {done}/{TOTAL_RECORDS} sent")

            if resp.status_code == 200:
                stats[resp.json().get("status", "other")] = (
                    stats.get(resp.json().get("status", "other"), 0) + 1
                )
            else:
                failures.append(f"stress-{i}: HTTP {resp.status_code} {resp.text[:80]}")

    await asyncio.gather(*(send(i) for i in range(1, TOTAL_RECORDS + 1)))
    return stats, failures


async def verify_db() -> tuple[int, int]:
    async with AsyncSessionLocal() as session:
        total = await session.scalar(
            select(func.count())
            .select_from(Product)
            .where(Product.source == SOURCE)
        )
        distinct = await session.scalar(
            select(func.count(func.distinct(Product.external_id))).where(
                Product.source == SOURCE
            )
        )
    return total, distinct


async def main() -> int:
    base_url = (
        sys.argv[1]
        if len(sys.argv) > 1
        else os.getenv("STRESS_BASE_URL", "http://127.0.0.1:8000")
    )

    cleared = await clear_previous_run()
    print(f"Stress test: {TOTAL_RECORDS} records, concurrency={CONCURRENCY}")
    print(f"Target:      {base_url}/ingest/rest")
    print(f"Cleared {cleared} rows from a previous run (source='{SOURCE}')")

    limits = httpx.Limits(
        max_connections=CONCURRENCY, max_keepalive_connections=CONCURRENCY
    )
    async with httpx.AsyncClient(limits=limits, timeout=30.0) as client:
        health = await client.get(f"{base_url}/health")
        health.raise_for_status()

        started = time.perf_counter()
        stats, failures = await send_all(client, base_url)
        elapsed = time.perf_counter() - started

    print("\n--- Results ---")
    print(f"Total time:   {elapsed:.2f}s")
    print(f"Throughput:   {TOTAL_RECORDS / elapsed:,.0f} records/sec")
    print(f"HTTP 200:     {stats.get('accepted', 0) + stats.get('updated', 0)}")
    print(f"  accepted:   {stats.get('accepted', 0)}")
    print(f"  updated:    {stats.get('updated', 0)}")
    print(f"Failures:     {len(failures)}")
    for failure in failures[:10]:
        print(f"  {failure}")

    total_rows, distinct_ids = await verify_db()
    print("\n--- Database verification ---")
    print(f"Rows (source='{SOURCE}'):        {total_rows}")
    print(f"Distinct external_id:   {distinct_ids}")

    ok = not failures and total_rows == TOTAL_RECORDS and distinct_ids == TOTAL_RECORDS
    print(
        f"\nVERIFICATION: {'PASS' if ok else 'FAIL'} — "
        f"{total_rows}/{TOTAL_RECORDS} rows, "
        f"{distinct_ids} distinct ids, {len(failures)} failed requests"
    )
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))