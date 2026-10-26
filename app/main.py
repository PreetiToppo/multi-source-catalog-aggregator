from contextlib import asynccontextmanager, suppress
from collections.abc import AsyncIterator
import asyncio

from fastapi import FastAPI

from app import models  # noqa: F401  # registers models on Base.metadata
from app.database import Base, engine
from app.routers import ingestion
from app.services import dlq


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    worker = dlq.start_retry_worker()
    yield
    dlq.stop_retry_worker()
    with suppress(asyncio.CancelledError):
        await worker


app = FastAPI(
    title="Multi-Source Product Catalog Aggregator",
    description="Aggregates product data from multiple external sources.",
    version="0.1.0",
    lifespan=lifespan,
)

app.include_router(ingestion.router)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}
