import csv
import io
import logging

from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
    Path,
    Request,
    UploadFile,
)
from fastapi.responses import JSONResponse
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_session
from app.schemas import (
    CsvIngestSummary,
    IngestResponse,
    QueuedForRetry,
    RestProductPayload,
)
from app.services import dlq
from app.services.ingestion import upsert_product, verify_webhook_signature

router = APIRouter(prefix="/ingest", tags=["ingestion"])
logger = logging.getLogger("app.ingestion")

SOURCE_REST = "rest"

CSV_FIELDS = {"external_id", "name", "price", "currency", "stock"}


@router.post("/rest", response_model=IngestResponse)
async def ingest_rest(
    payload: RestProductPayload,
    session: AsyncSession = Depends(get_session),
) -> IngestResponse:
    """Ingest a single product over REST; the source is fixed to ``"rest"``."""
    result = await upsert_product(session, source=SOURCE_REST, payload=payload)
    return IngestResponse(**result)


@router.post("/csv", response_model=CsvIngestSummary)
async def ingest_csv(
    file: UploadFile,
    session: AsyncSession = Depends(get_session),
) -> CsvIngestSummary:
    """Ingest products from a CSV upload (source is fixed to `"csv"`).

    Expected header: `external_id,name,price,currency,stock`.
    Each data row is validated and upserted independently; invalid rows are
    collected in `errors` (row numbers are 1-based, header not counted).
    """
    raw = await file.read()
    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError:
        raise HTTPException(status_code=400, detail="File is not valid UTF-8")

    reader = csv.DictReader(io.StringIO(text))
    if not reader.fieldnames or not CSV_FIELDS.issubset(reader.fieldnames):
        raise HTTPException(
            status_code=400,
            detail=f"CSV must have header: {','.join(sorted(CSV_FIELDS))}",
        )

    inserted = 0
    updated = 0
    errors: list[dict] = []

    for row_num, row in enumerate(reader, start=1):
        data = {
            key: value.strip() if isinstance(value, str) else value
            for key, value in row.items()
            if key in CSV_FIELDS
        }
        try:
            payload = RestProductPayload.model_validate(data)
        except ValidationError as e:
            first = e.errors()[0]
            errors.append(
                {"row": row_num, "reason": f"{'.'.join(str(k) for k in first['loc'])}: {first['msg']}"}
            )
            continue

        try:
            result = await upsert_product(session, source="csv", payload=payload)
        except Exception as e:  # noqa: BLE001
            errors.append({"row": row_num, "reason": f"db error: {e}"})
            continue

        if result["status"] == "accepted":
            inserted += 1
        else:
            updated += 1

    return CsvIngestSummary(inserted=inserted, updated=updated, errors=errors)


@router.post(
    "/webhook/{source_name}",
    response_model=IngestResponse,
    responses={202: {"model": QueuedForRetry}},
)
async def ingest_webhook(
    request: Request,
    source_name: str = Path(..., max_length=64),
    session: AsyncSession = Depends(get_session),
) -> IngestResponse | JSONResponse:
    """Ingest a single product from a webhook; `source_name` (path) sets the source.

    Expects the same JSON body as /ingest/rest plus an optional
    `X-Signature-256` header: HMAC-SHA256 of the raw body with WEBHOOK_SECRET.
    Signature checking is a placeholder — mismatches are logged but processed.

    If the upsert fails, the payload is saved to failed_webhooks
    (status="pending") and the request returns 202 {"status": "queued_for_retry"};
    the background retry worker (every 30s, max 5 attempts) processes it later.
    """
    body = await request.body()
    verify_webhook_signature(body, request.headers.get("X-Signature-256"))

    try:
        payload = RestProductPayload.model_validate_json(body)
    except ValidationError as e:
        # include_input=False: the raw bytes input is not JSON-serializable
        # and would turn this client error into a 500 response.
        raise HTTPException(status_code=422, detail=e.errors(include_input=False))

    try:
        result = await upsert_product(session, source=source_name, payload=payload)
    except Exception as e:  # noqa: BLE001
        logger.warning("webhook upsert failed for source=%s: %s", source_name, e)
        try:
            await dlq.save_failed_webhook(
                source=source_name,
                payload=payload.model_dump(mode="json"),
                error_message=str(e),
            )
        except Exception:  # noqa: BLE001
            logger.exception("could not persist failed webhook for source=%s", source_name)
            return JSONResponse(
                status_code=503,
                content={"status": "queue_unavailable"},
            )
        return JSONResponse(
            status_code=202,
            content=QueuedForRetry(status="queued_for_retry").model_dump(),
        )

    return IngestResponse(**result)
