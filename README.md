# doomsday-predict-analytics

Backend data pipeline and API for the Doomsday Prediction Analytics platform.
Pulls binary yes/no prediction market data from [Polymarket](https://polymarket.com),
stores it in BigQuery, exports it to GCS, and serves it via a REST API.

---

## Architecture

```
Polymarket API
    │
    ▼
doomsday-polymarket (Cloud Run Job)
    │  reads market configs from doomsday.markets BQ table
    │  writes snapshots to doomsday.market_snapshots BQ table
    │  exports JSON to gs://doomsday-data (GCS)
    │
doomsday-api (Cloud Run Service)
    │  CRUD on doomsday.markets
    │  reads doomsday.market_snapshots
    │  triggers doomsday-polymarket on demand
    │
    ▼
Frontend Admin (doomsday-predict-frontend-admin)
```

---

## GCP Infrastructure

| Resource | Value |
|---|---|
| GCP Project | `fg-polylabs` |
| Region | `us-central1` |
| BigQuery Dataset | `fg-polylabs.doomsday` |
| GCS Bucket | `gs://doomsday-data` |
| Artifact Registry | `us-central1-docker.pkg.dev/fg-polylabs/doomsday/` |
| Cloud Run Job (collector) | `doomsday-polymarket` |
| Cloud Run Job (exporter) | `doomsday-exporter` |
| Cloud Run Service (API) | `doomsday-api` |
| Cloud Scheduler | `doomsday-daily` (1 AM UTC) |
| Service Account | `doomsday-runner@fg-polylabs.iam.gserviceaccount.com` |

---

## BigQuery Tables

### `fg-polylabs.doomsday.markets`

Source of truth for which Polymarket events the pipeline tracks.
Managed via the API or directly in BQ Console.

| Column | Type | Description |
|---|---|---|
| `id` | STRING | Unique row ID (hex) |
| `slug` | STRING | Exact Polymarket event slug (single-event mode) |
| `tag` | STRING | Polymarket tag slug (series-discovery mode) |
| `slug_prefix` | STRING | Prefix filter applied to tag results |
| `category` | STRING | Display category (e.g. Geopolitics, AI & Tech) |
| `active` | BOOL | Whether to include in daily runs |
| `created_at` | TIMESTAMP | Row creation time (UTC) |
| `updated_at` | TIMESTAMP | Last modification time (UTC) |

Set `slug` for a single named event, or `tag` + `slug_prefix` to auto-discover
a series of events sharing a tag.

### `fg-polylabs.doomsday.market_snapshots`

Immutable append-only price history. Deduplicated on `(event_slug, snapshot_timestamp)`.

| Column | Type | Description |
|---|---|---|
| `event_slug` | STRING | Polymarket event slug |
| `event_title` | STRING | Human-readable event title |
| `question` | STRING | Market question (outcome text) |
| `category` | STRING | Category from market config |
| `snapshot_date` | DATE | UTC date of snapshot |
| `snapshot_timestamp` | TIMESTAMP | UTC time of snapshot |
| `expiration_timestamp` | TIMESTAMP | Market resolution time (nullable) |
| `yes_price` | FLOAT | YES outcome price (0–1) |
| `no_price` | FLOAT | NO outcome price (0–1) |
| `best_bid` | FLOAT | Best bid (nullable; NULL on backfills) |
| `best_ask` | FLOAT | Best ask (nullable) |
| `spread` | FLOAT | best_ask − best_bid (nullable) |
| `volume_24h` | FLOAT | 24h trading volume (nullable) |
| `volume_total` | FLOAT | All-time trading volume (nullable) |
| `liquidity` | FLOAT | Current liquidity (nullable) |

---

## Repository Layout

```
cmd/
  doomsday/   — data collection CLI (Cloud Run Job)
  exporter/   — GCS/Drive export CLI (Cloud Run Job)
  api/        — HTTP API server (Cloud Run Service)
internal/doomsday/
  client.go       — Polymarket Gamma & CLOB API client
  models.go       — shared data types
  loader.go       — BigQuery MERGE loader (market_snapshots)
  snapshots.go    — BQ read queries for the API
  market_store.go — CRUD on doomsday.markets BQ table
  export.go       — GCS and Google Drive exporter
scripts/
  run.sh      — gcloud helpers for running/scheduling jobs
Dockerfile          — doomsday-polymarket image
Dockerfile.exporter — doomsday-exporter image
Dockerfile.api      — doomsday-api image
```

---

## API Reference

Base URL: `https://doomsday-api-<hash>-uc.a.run.app` (Cloud Run Service URL)

Set `Authorization: Bearer <API_KEY>` if the `API_KEY` env var is configured.

### Health

```
GET /api/v1/health
→ 200 { "status": "ok", "time": "..." }
```

### Markets (config table)

```
GET /api/v1/markets[?active=true]
→ 200 { "markets": [ MarketConfig, ... ] }

POST /api/v1/markets
Body: { "slug": "...", "category": "...", "active": true }
  OR  { "tag": "...", "slug_prefix": "...", "category": "...", "active": true }
→ 201 MarketConfig

GET /api/v1/markets/{id}
→ 200 MarketConfig | 404

PATCH /api/v1/markets/{id}
Body: { "category": "...", "active": false }   (both fields optional)
→ 200 MarketConfig | 404

DELETE /api/v1/markets/{id}
→ 204 | 404
```

### Events (snapshot data)

```
GET /api/v1/events
→ 200 { "events": [ EventSummary, ... ] }

GET /api/v1/events/{slug}
→ 200 EventDetail | 404
```

`EventSummary` fields: `event_slug`, `event_title`, `category`, `snapshot_count`, `latest_snapshot`

`EventDetail` fields: `event_slug`, `event_title`, `category`, `snapshots[]`
- Each snapshot: `question`, `snapshot_timestamp`, `expiration_timestamp`, `yes_price`, `no_price`

### Jobs

```
POST /api/v1/jobs/run
Body: {
  "mode": "all" | "slug" | "tag",   // default: "all"
  "slug": "...",                     // required for mode=slug
  "tag": "...",                      // required for mode=tag
  "category": "...",                 // optional
  "yesterday": false,
  "active_only": true,
  "no_volume": false
}
→ 202 { "message": "job triggered", "args": [...] }
```

---

## Data Collection CLI (`cmd/doomsday`)

```bash
# Daily incremental — all active markets from doomsday.markets (used by cron)
doomsday --all --yesterday --active-only

# Historical backfill — all markets, full history, no volume fields
doomsday --all --no-volume

# Single event by slug
doomsday --slug=us-x-iran-ceasefire-before-july --category=Geopolitics

# Discover events by tag
doomsday --tag=us-iran --category=Geopolitics

# Flags
--fidelity=60       Price snapshot interval in minutes (default: 60)
--dry-run           Print JSONL to stdout instead of writing to BigQuery
--no-volume         NULL out bid/ask/volume/liquidity (for backfills)
--start-date        Override start date YYYY-MM-DD
--end-date          Override end date YYYY-MM-DD (exclusive)
--yesterday         Yesterday 00:00–00:00 UTC window, fidelity=1440
--active-only       Skip closed/resolved markets
```

---

## Scheduled Job

One Cloud Scheduler job drives all daily collection:

| Name | Schedule | What it does |
|---|---|---|
| `doomsday-daily` | `0 1 * * *` (1 AM UTC) | Runs `doomsday --all --yesterday --active-only` |

To create it:
```bash
./scripts/run.sh schedule-daily
```

---

## Local Operations

```bash
# Run daily incremental immediately
./scripts/run.sh daily

# Historical backfill (all active markets)
./scripts/run.sh backfill

# Run for a specific slug
./scripts/run.sh execute --slug=us-x-iran-ceasefire-before-july --category=Geopolitics

# Trigger GCS export manually
./scripts/run.sh export

# List Cloud Scheduler jobs
./scripts/run.sh list-schedules
```

---

## Seeding the markets Table

On first deploy, seed the `doomsday.markets` table via the API:

```bash
API_URL="https://doomsday-api-<hash>-uc.a.run.app"

# Single-event mode
curl -X POST "$API_URL/api/v1/markets" \
  -H "Content-Type: application/json" \
  -d '{"slug":"trump-out-as-president-before-2027","category":"Politics","active":true}'

# Series-discovery mode
curl -X POST "$API_URL/api/v1/markets" \
  -H "Content-Type: application/json" \
  -d '{"tag":"us-iran","slug_prefix":"us-x-iran-ceasefire","category":"Geopolitics","active":true}'
```

Or run `./scripts/run.sh backfill` after seeding to load historical data.

---

## Deployment

Pushes to `main` trigger the CI/CD pipeline (`.github/workflows/deploy.yml`), which:
1. Builds and pushes all three Docker images to Artifact Registry
2. Updates `doomsday-polymarket` and `doomsday-exporter` Cloud Run Jobs
3. Deploys/updates `doomsday-api` Cloud Run Service

### Required GitHub Secrets
- `WIF_PROVIDER` — Workload Identity Federation provider resource name

### Required `go mod tidy`

After cloning, run:
```bash
go mod tidy
```
to ensure `golang.org/x/oauth2` is listed as a direct dependency (used by `cmd/api`).
