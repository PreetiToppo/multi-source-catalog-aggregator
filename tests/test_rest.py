"""Tests for POST /ingest/rest and /health."""

from sqlalchemy import select

from app.database import AsyncSessionLocal
from app.models import Product

PAYLOAD = {
    "external_id": "T-100",
    "name": "Test Widget",
    "price": "19.99",
    "currency": "USD",
    "stock": 5,
}


async def test_health(client) -> None:
    resp = await client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


async def test_rest_inserts_new_product(client) -> None:
    resp = await client.post("/ingest/rest", json=PAYLOAD)
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "accepted"
    assert body["product_id"] >= 1

    async with AsyncSessionLocal() as session:
        product = await session.scalar(
            select(Product).where(
                Product.source == "rest", Product.external_id == "T-100"
            )
        )
    assert product is not None
    assert product.name == "Test Widget"
    assert float(product.price) == 19.99


async def test_rest_updates_existing_product(client) -> None:
    first = await client.post("/ingest/rest", json=PAYLOAD)
    product_id = first.json()["product_id"]
    changed = {**PAYLOAD, "price": "24.99", "stock": 3}
    resp = await client.post("/ingest/rest", json=changed)
    assert resp.status_code == 200
    assert resp.json() == {"status": "updated", "product_id": product_id}

    async with AsyncSessionLocal() as session:
        rows = (await session.execute(select(Product))).scalars().all()
    assert len(rows) == 1
    assert float(rows[0].price) == 24.99
    assert rows[0].stock == 3


async def test_rest_missing_name_is_rejected(client) -> None:
    resp = await client.post("/ingest/rest", json={k: v for k, v in PAYLOAD.items() if k != "name"})
    assert resp.status_code == 422


async def test_rest_negative_price_is_rejected(client) -> None:
    resp = await client.post("/ingest/rest", json={**PAYLOAD, "price": "-1"})
    assert resp.status_code == 422


async def test_rest_empty_name_is_rejected(client) -> None:
    resp = await client.post("/ingest/rest", json={**PAYLOAD, "name": ""})
    assert resp.status_code == 422


async def test_rest_leaves_no_rows_when_rejected(client) -> None:
    await client.post("/ingest/rest", json={**PAYLOAD, "price": "-1"})
    async with AsyncSessionLocal() as session:
        count = len((await session.execute(select(Product))).scalars().all())
    assert count == 0