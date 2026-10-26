"""Tests for POST /ingest/csv."""

from sqlalchemy import select

import app.routers.ingestion as ingestion_router
from app.database import AsyncSessionLocal
from app.models import Product
from app.services.ingestion import upsert_product as real_upsert

VALID_CSV = (
    b"external_id,name,price,currency,stock\n"
    b"CSV-1,Screwdriver,4.99,USD,100\n"
    b"CSV-2,Hammer,12.50,USD,40\n"
)

MIXED_CSV = (
    b"external_id,name,price,currency,stock\n"
    b"CSV-1,Screwdriver,4.99,USD,100\n"
    b"CSV-2,Hammer,12.50,USD,40\n"
    b"CSV-3,,7.25,USD,10\n"
)


async def test_csv_two_valid_rows_inserted(client) -> None:
    resp = await client.post(
        "/ingest/csv", files={"file": ("products.csv", VALID_CSV, "text/csv")}
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body == {"inserted": 2, "updated": 0, "errors": []}

    async with AsyncSessionLocal() as session:
        rows = (await session.execute(select(Product))).scalars().all()
    assert len(rows) == 2
    assert {r.source for r in rows} == {"csv"}


async def test_csv_malformed_row_is_isolated(client) -> None:
    resp = await client.post(
        "/ingest/csv", files={"file": ("products.csv", MIXED_CSV, "text/csv")}
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["inserted"] == 2
    assert body["updated"] == 0
    assert len(body["errors"]) == 1
    assert body["errors"][0]["row"] == 3
    assert "name" in body["errors"][0]["reason"]

    async with AsyncSessionLocal() as session:
        rows = (await session.execute(select(Product))).scalars().all()
    assert len(rows) == 2  # malformed row wrote nothing


async def test_csv_second_upload_updates(client) -> None:
    await client.post(
        "/ingest/csv", files={"file": ("products.csv", VALID_CSV, "text/csv")}
    )
    updated_csv = (
        b"external_id,name,price,currency,stock\n"
        b"CSV-1,Screwdriver Pro,5.99,USD,90\n"
    )
    resp = await client.post(
        "/ingest/csv", files={"file": ("products.csv", updated_csv, "text/csv")}
    )
    assert resp.json() == {"inserted": 0, "updated": 1, "errors": []}

    async with AsyncSessionLocal() as session:
        row = await session.scalar(select(Product).where(Product.external_id == "CSV-1"))
    assert row.name == "Screwdriver Pro"
    assert float(row.price) == 5.99


async def test_csv_bad_header_rejected(client) -> None:
    resp = await client.post(
        "/ingest/csv",
        files={"file": ("products.csv", b"foo,bar\n1,2\n", "text/csv")},
    )
    assert resp.status_code == 400
    assert "CSV must have header" in resp.json()["detail"]


async def test_csv_non_utf8_rejected(client) -> None:
    resp = await client.post(
        "/ingest/csv",
        files={"file": ("products.csv", b"\xff\xfe\x00garbage", "text/csv")},
    )
    assert resp.status_code == 400
    assert resp.json()["detail"] == "File is not valid UTF-8"


async def test_csv_db_error_recorded_per_row(client, monkeypatch) -> None:
    async def failing_upsert(session, source, payload):
        if payload.external_id == "CSV-2":
            raise RuntimeError("db down")
        return await real_upsert(session, source, payload)

    monkeypatch.setattr(ingestion_router, "upsert_product", failing_upsert)
    resp = await client.post(
        "/ingest/csv", files={"file": ("products.csv", MIXED_CSV, "text/csv")}
    )
    body = resp.json()
    # CSV-1 ok, CSV-2 db error, CSV-3 invalid name
    assert body["inserted"] == 1
    assert len(body["errors"]) == 2
    assert {"row": 2, "reason": "db error: db down"} in body["errors"]
    assert {"row": 3, "reason": "name: String should have at least 1 character"} in body["errors"]