# Multi-Source Product Catalog Aggregator

A FastAPI service that consolidates product records from multiple external
sources into a single PostgreSQL catalog. Data can arrive over a REST API, a
CSV upload, or signed webhooks; every write is an idempotent upsert, and failed
webhook deliveries are retried through a dead-letter queue.

## Architecture

```
             ┌────────────────────────────── FastAPI ───────────────────────────────┐
             │                                                                      │
 POST /ingest/rest ──┐                                                              │
 POST /ingest/csv  ──┼──▶ RestProductPayload ──▶ upsert_product() ──▶ PostgreSQL    │
 POST /ingest/webhook┘        (Pydantic)        ON CONFLICT          products      │
        │                                          │                                │
        │ upsert fails                             │ accepted / updated             │
        ▼                                          ▼                                │
  failed_webhooks ◀── 202 queued_for_retry    {status, product_id}                  │
        │                                                                      │
        │  retry worker (asyncio, every 30s, max 5 attempts)                   │
        ├──▶ completed   (payload ingested successfully)                      │
        └──▶ dead_letter  (5 failed attempts — parked for inspection)         │
             └────────────────────────────────────────────────────────────────┘
```

**Ingestion surfaces**

| Endpoint | Source label | Notes |
|---|---|---|
| `POST /ingest/rest` | `rest` | Single JSON product; strict Pydantic validation |
| `POST /ingest/csv` | `csv` | Multipart file upload; per-row validation and error isolation |
| `POST /ingest/webhook/{source}` | path parameter | JSON body + optional HMAC `X-Signature-256` header; degrades to HTTP 202 on failure |

**Idempotent upsert.** All three surfaces funnel into a single
`upsert_product()` helper that executes a PostgreSQL
`INSERT ... ON CONFLICT (source, external_id) DO UPDATE`. Replaying the same
payload any number of times converges on exactly one row, and the response
distinguishes a fresh insert (`accepted`) from an overwrite (`updated`) via
Postgres's `xmax = 0` trick in the `RETURNING` clause — no read-then-write
race.

**Dead-letter queue.** When a webhook upsert fails, the raw payload is
persisted to `failed_webhooks` and the client receives `202 Accepted` instead
of a 500. A background worker retries pending rows every 30 seconds; after 5
failed attempts a row is marked `dead_letter` and excluded from further
retries, keeping failures observable instead of silently looping.

**Stack:** Python 3.11 · FastAPI · SQLAlchemy 2 (async) · asyncpg · PostgreSQL 16 · Redis · Docker Compose

## Project structure

```
app/
├── main.py                 FastAPI app, lifespan (schema bootstrap + worker)
├── database.py             Async engine, session factory, declarative base
├── models.py               Product and FailedWebhook ORM models
├── schemas.py              Pydantic request/response models
├── routers/
│   └── ingestion.py        REST, CSV and webhook endpoints
└── services/
    ├── ingestion.py        Idempotent upsert + webhook signature verification
    └── dlq.py              Retry worker and dead-letter bookkeeping
tests/
├── conftest.py             Async client fixture, per-test DB cleanup
├── test_rest.py            REST ingestion and validation
├── test_csv.py             CSV parsing, per-row error isolation
├── test_webhook.py         Signatures, 202 queueing, failure paths
└── test_dlq.py             Retry lifecycle: pending → completed / dead_letter
Dockerfile
docker-compose.yml
requirements.txt
.env.example
```

## How to run

### Option A — Docker Compose (recommended)

```bash
cp .env.example .env        # then edit DATABASE_URL / REDIS_URL / WEBHOOK_SECRET
docker compose up --build
```

Services:

| Service | URL |
|---|---|
| API + Swagger docs | http://localhost:8001/docs (host port configurable with `APP_PORT`) |
| Health check | http://localhost:8001/health |
| PostgreSQL | `localhost:5433` |
| Redis | `localhost:6379` |

The container runs `uvicorn app.main:app --host 0.0.0.0 --port 8000`;
`docker-compose.yml` maps it to host port 8001 by default (override with
`APP_PORT=8000 docker compose up`). The `app` service starts only after the
`db` and `redis` healthchecks pass and restarts on failure.

### Option B — Local uvicorn

```bash
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt

docker compose up -d db redis      # infrastructure only
uvicorn app.main:app --reload      # http://localhost:8000
```

## API examples

### 1. REST

```bash
curl -s -X POST http://localhost:8001/ingest/rest \
  -H "Content-Type: application/json" \
  -d '{"external_id": "SKU-1001", "name": "Widget Pro", "price": "19.99", "currency": "USD", "stock": 5}'
# {"status":"accepted","product_id":1}

# Same payload again -> idempotent update, still one row:
# {"status":"updated","product_id":1}
```

### 2. CSV upload

Header must be `external_id,name,price,currency,stock`. Invalid rows are
reported individually without aborting the batch.

```bash
cat > products.csv <<'EOF'
external_id,name,price,currency,stock
CSV-1,Screwdriver,4.99,USD,100
CSV-2,Hammer,12.50,USD,40
CSV-3,,7.25,USD,10
EOF

curl -s -X POST http://localhost:8001/ingest/csv -F "file=@products.csv"
# {"inserted":2,"updated":0,"errors":[{"row":3,"reason":"name: String should have at least 1 character"}]}
```

### 3. Webhook (HMAC-signed)

```bash
BODY='{"external_id":"WH-500","name":"Webhook Widget","price":"33.00","currency":"USD","stock":9}'
SIG=$(printf '%s' "$BODY" | openssl dgst -sha256 -hmac "change-me" | awk '{print $NF}')

curl -s -X POST http://localhost:8001/ingest/webhook/shopify \
  -H "Content-Type: application/json" \
  -H "X-Signature-256: sha256=$SIG" \
  -d "$BODY"
# {"status":"accepted","product_id":3}
```

The `source_name` path segment becomes the product's `source`, so each
integration keeps its own namespace. If the upsert fails the endpoint answers
`202 {"status":"queued_for_retry"}` and the retry worker takes over.

### Response semantics

| Status | HTTP | Meaning |
|---|---|---|
| `accepted` | 200 | New row inserted |
| `updated` | 200 | Existing row overwritten |
| `queued_for_retry` | 202 | Upsert failed; payload stored in `failed_webhooks` |
| `queue_unavailable` | 503 | Upsert and queue write both failed |

## Testing

The suite runs against real PostgreSQL — start the database first
(`docker compose up -d db`), then:

```bash
pytest --cov=app --cov-report=term-missing
```

**94% test coverage across 34 tests**, covering all three ingestion surfaces,
validation failures, HMAC verification, and the full retry lifecycle
(`pending → completed`, `dead_letter` after 5 attempts).

## Known limitations

- **Schema management is dev-only.** Startup runs
  `Base.metadata.create_all`, which creates tables but never alters them.
  Production deployments should use [Alembic](https://alembic.sqlalchemy.org/)
  migrations.
- **Flat retry schedule.** The worker retries every 30 seconds with no
  exponential backoff or jitter; a long outage will burn all 5 attempts
  quickly.
- **Single-process worker.** The retry loop is an in-process asyncio task —
  running multiple app replicas would need a distributed lock or a shared job
  queue to avoid duplicate retries.
- **Advisory webhook signatures.** Signature mismatches are logged but not
  rejected, and one static `WEBHOOK_SECRET` is shared by all sources;
  enforcement and per-source secrets are future work.
- **Redis is provisioned but unused.** It is wired into Compose for future
  caching and rate limiting; nothing reads it yet.
- **No authentication.** Endpoints are open; add auth before exposing the
  service beyond localhost.