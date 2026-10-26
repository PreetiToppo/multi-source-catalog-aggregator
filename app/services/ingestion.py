import hashlib
import hmac
import logging
import os
from sqlalchemy import literal_column
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Product
from app.schemas import RestProductPayload

logger = logging.getLogger("app.ingestion")


def verify_webhook_signature(body: bytes, signature: str | None) -> bool:
    """Verify the HMAC-SHA256 signature of a webhook request body.

    Verification is advisory: a missing secret, missing header, or mismatch
    is logged and the payload is still processed. Enforcement (rejecting
    unsigned requests) is deferred to a later hardening pass.
    """
    secret = os.getenv("WEBHOOK_SECRET")
    if not secret:
        logger.warning("WEBHOOK_SECRET not set; webhook processed without verification")
        return True
    if not signature:
        logger.warning("webhook missing X-Signature-256 header; processing anyway")
        return False
    expected = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    supplied = signature.removeprefix("sha256=")
    if not hmac.compare_digest(expected, supplied):
        logger.warning("webhook signature mismatch; processing anyway")
        return False
    return True


async def upsert_product(
    session: AsyncSession, source: str, payload: RestProductPayload
) -> dict:
    """Insert or update a product keyed on (source, external_id).

    Returns {"status": "accepted"|"updated", "product_id": int}.
    "accepted" -> row was newly inserted
    "updated"  -> row already existed and was overwritten (upsert semantics)
    """
    values = {
        "source": source,
        "external_id": payload.external_id,
        "name": payload.name,
        "price": payload.price,
        "currency": payload.currency,
        "stock": payload.stock,
    }
    stmt = (
        insert(Product)
        .values(**values)
        .on_conflict_do_update(
            index_elements=["source", "external_id"],
            set_={
                "name": payload.name,
                "price": payload.price,
                "currency": payload.currency,
                "stock": payload.stock,
            },
        )
        # xmax = 0 is true only for a fresh insert; an ON CONFLICT UPDATE
        # leaves a non-zero xmax on the row version it wrote.
        .returning(
            Product.id,
            Product.updated_at,
            literal_column("xmax = 0").label("is_new"),
        )
    )
    result = await session.execute(stmt)
    row = result.fetchone()
    await session.commit()
    return {
        "status": "accepted" if row.is_new else "updated",
        "product_id": row.id,
    }
