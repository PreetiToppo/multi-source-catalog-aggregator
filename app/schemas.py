from datetime import datetime
from decimal import Decimal
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class RestProductPayload(BaseModel):
    """Shared payload shape for all ingestion endpoints."""

    external_id: str = Field(..., min_length=1, max_length=255)
    name: str = Field(..., min_length=1, max_length=512)
    price: Decimal = Field(..., ge=0)
    currency: str = Field(..., min_length=3, max_length=8)
    stock: int = Field(..., ge=0)


class IngestResponse(BaseModel):
    status: Literal["accepted", "updated"]
    product_id: int


class QueuedForRetry(BaseModel):
    status: Literal["queued_for_retry"]


class CsvIngestError(BaseModel):
    row: int = Field(..., description="1-based data row number (header not counted)")
    reason: str


class CsvIngestSummary(BaseModel):
    inserted: int
    updated: int
    errors: list[CsvIngestError]


class ProductIngest(BaseModel):
    """Payload for ingesting a single product from an external source."""

    source: str = Field(..., max_length=64, description="Identifier of the data source")
    external_id: str = Field(
        ..., max_length=255, description="Product ID as used by the source"
    )
    sku: str | None = Field(None, max_length=128)
    name: str = Field(..., max_length=512)
    price: float | None = Field(None, ge=0)
    currency: str | None = Field(None, max_length=8, min_length=3)
    stock: int | None = Field(None, ge=0)
    raw_data: dict[str, Any] | None = Field(
        None, description="Original source payload for traceability"
    )


class ProductIngestResult(BaseModel):
    id: int
    source: str
    external_id: str


class ProductOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    source: str
    external_id: str
    sku: str | None
    name: str
    price: float | None
    currency: str | None
    stock: int | None
    raw_data: dict[str, Any] | None
    created_at: datetime
    updated_at: datetime
