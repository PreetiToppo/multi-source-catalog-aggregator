"""Shared pytest fixtures: app client + database lifecycle."""

import os
import sys
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")

import httpx  # noqa: E402
import pytest_asyncio  # noqa: E402
from sqlalchemy import delete  # noqa: E402

from app.database import AsyncSessionLocal, Base, engine  # noqa: E402
from app.main import app  # noqa: E402
from app.models import FailedWebhook, Product  # noqa: E402


@pytest_asyncio.fixture(scope="session", autouse=True)
async def create_tables() -> None:
    """Create tables once per test session (ASGITransport runs no lifespan)."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield


@pytest_asyncio.fixture(scope="session")
async def client():
    """Async test client bound to the FastAPI app via ASGI transport."""
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://testserver"
    ) as async_client:
        yield async_client


@pytest_asyncio.fixture(autouse=True)
async def clean_db() -> None:
    """Wipe ingestion tables before every test so tests stay independent."""
    async with AsyncSessionLocal() as session:
        await session.execute(delete(FailedWebhook))
        await session.execute(delete(Product))
        await session.commit()
    yield