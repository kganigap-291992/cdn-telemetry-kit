# CDN Telemetry Kit — Telemetry Contract & Data Pipeline 

This repo generates **deterministic, synthetic CDN-like telemetry** and provides a **ClickHouse-friendly ingestion pipeline** for two layers:

- **`raw_minute`** — minute-level aggregated telemetry (highest fidelity)
- **`agg_15m`** — 15-minute aggregated telemetry (graph + long-window friendly)

**Compatibility rule (Frozen):**  
✅ Never rename columns. ✅ Never reorder existing columns. ✅ Only add new columns (additive evolution).

---

## High-level architecture

```
VPS (or any Linux host)
  ├─ cdn-telemetry-kit/           # this repo
  │   ├─ src/telemetry_kit/       # generator + schema + aggregation
  │   └─ scripts/                # emit + ingest scripts
  ├─ .venv/                       # python deps (numpy, pandas)
  └─ Docker
      └─ clickhouse-server        # ClickHouse database (container)
```

### Data flow

- Python generator emits **JSONEachRow** (one JSON object per line)
- Output is piped into ClickHouse via `clickhouse-client`:

```
python emit_*.py | docker exec -i clickhouse clickhouse-client --query "INSERT ... FORMAT JSONEachRow"
```

### Retention / deletion

Both ClickHouse tables are configured with:

- `PARTITION BY toYYYYMMDD(ts)`
- `TTL ts + toIntervalDay(30)`

Meaning rows expire automatically after **30 days** (ClickHouse removes expired data during background merges).

---

## Layers & tables

### 1) `raw_minute` (generator output)
Minute-level aggregated telemetry. Each row = **1 minute × 1 slice**.

Slice dimensions:
- `partner`, `service`, `region`, `pop`, `host`, `content_type`, `ua_family`

Core metrics:
- requests, bytes, cache hit rate, latency percentiles, HTTP buckets, detailed HTTP codes, crc errors

**ClickHouse table:** `cachey.raw_minute`

### 2) `agg_15m` (aggregated for graphs / long windows)
15-minute bucket telemetry aggregated from `raw_minute`, keeping the same slice dimensions.

**ClickHouse table:** `cachey.agg_15m`

---

## Canonical allowed values (frozen)

These are intentionally small, stable lists (additive only):

- `DEFAULT_SERVICES = ["live","vod","dvr","eas","live_ott","app_backend"]`
- `DEFAULT_CONTENT_TYPES = ["manifest","segment","api"]`
- `DEFAULT_REGIONS = ["us-east","us-west","us-central","eu-west","eu-central","ap-south","ap-northeast","sa-east"]`
- `DEFAULT_UA_FAMILIES = ["stb","mobile","web","smart_tv","console"]`

---

## Column contracts (frozen order)

### `RAW_MINUTE_COLUMNS`
Stable column order for `raw_minute`. **Never rename or reorder; only add.**

- Provenance: `seed`
- Slice dims: `ts, partner, service, region, pop, host, content_type, ua_family`
- Core metrics: `requests, bytes_sent, p50_ms, p95_ms, p99_ms, cache_hit_rate`
- Status buckets: `http_2xx_count, http_3xx_count, http_4xx_count, http_5xx_count`
- Detailed status: `status_200, status_206, status_304, status_403, status_404, status_429, status_500, status_502, status_503, status_504`
- Other: `crc_errors`

### `AGG_15M_COLUMNS`
Stable column order for `agg_15m`. Intentionally matches `RAW_MINUTE_COLUMNS` field names to keep downstream compatibility.

Same fields as above; `ts` represents the **15m bucket start**.

---

## Generator guarantees (invariants)

The generator and aggregator guarantee:

- `http_2xx_count + http_3xx_count + http_4xx_count + http_5xx_count == requests`
- `status_200 + status_206 == http_2xx_count`
- `status_304 == http_3xx_count`
- `status_403 + status_404 + status_429 == http_4xx_count`
- `status_500 + status_502 + status_503 + status_504 == http_5xx_count`
- `p95_ms >= p50_ms` and `p99_ms >= p95_ms`

---

## Local setup (Linux / VPS)

### 0) Clone repo

```bash
git clone https://github.com/<your-user>/cdn-telemetry-kit.git
cd cdn-telemetry-kit
```

### 1) Create Python venv + install deps

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install numpy pandas
```

Sanity check:

```bash
python -c "import numpy, pandas; print('deps ok')"
```

---

## ClickHouse (Docker) setup

> This repo assumes ClickHouse is reachable via `docker exec -i clickhouse clickhouse-client ...`

### Check container is running

```bash
docker ps
```

Enter client:

```bash
docker exec -it clickhouse clickhouse-client
```

---

## Tables (schema + TTL)

### Verify `raw_minute` table definition

```bash
docker exec -i clickhouse clickhouse-client --query "SHOW CREATE TABLE cachey.raw_minute"
```

Expected features:
- `ENGINE = MergeTree`
- `PARTITION BY toYYYYMMDD(ts)`
- `ORDER BY (partner, service, region, pop, ts, content_type, ua_family, host)`
- `TTL ts + toIntervalDay(30)`

### Verify `agg_15m` table definition

```bash
docker exec -i clickhouse clickhouse-client --query "SHOW CREATE TABLE cachey.agg_15m"
```

Expected:
- same partition/order patterns
- same TTL: `ts + 30 days`

---

## Emit scripts (JSONEachRow)

### Emit minute JSONEachRow (stdout)

```bash
source .venv/bin/activate
python scripts/emit_json_eachrow.py --minutes 5 --start "2026-02-20T00:00:00Z" --seed 7 --density 0.10 | head -n 2
```

### Emit 15m JSONEachRow (stdout)

```bash
source .venv/bin/activate
python scripts/emit_agg15m_json_each_row.py --minutes 60 --bucket 15 --start "2026-02-20T00:00:00Z" --seed 7 --density 0.10 | head -n 2
```

---

## Insert into ClickHouse

### Insert 15m buckets into `cachey.agg_15m`

```bash
source .venv/bin/activate
python scripts/emit_agg15m_json_each_row.py \
  --minutes 60 \
  --bucket 15 \
  --start "2026-02-20T00:00:00Z" \
  --seed 7 \
  --density 0.10 \
| docker exec -i clickhouse clickhouse-client --query "INSERT INTO cachey.agg_15m FORMAT JSONEachRow"
```

Verify buckets:

```bash
docker exec -i clickhouse clickhouse-client --query "
SELECT ts, count() rows, sum(requests) req
FROM cachey.agg_15m
GROUP BY ts
ORDER BY ts
LIMIT 10
"
```

---

## Verification commands (recommended)

### 1) Row span / retention check (raw_minute)

```bash
docker exec -i clickhouse clickhouse-client --query "
SELECT
  min(ts) AS oldest,
  max(ts) AS newest,
  dateDiff('day', min(ts), max(ts)) AS span_days,
  count() AS rows
FROM cachey.raw_minute
"
```

### 2) 15m “is cron keeping up?” check

```bash
docker exec -i clickhouse clickhouse-client --query "
SELECT
  max(ts) AS newest_ts,
  dateDiff('minute', max(ts), now()) AS minutes_behind
FROM cachey.agg_15m
"
```

Note: If you intentionally ingest **yesterday only**, `minutes_behind` will typically be close to ~24 hours.

### 3) Bucket count per day (agg_15m)

```bash
docker exec -i clickhouse clickhouse-client --query "
SELECT
  toDate(ts) AS d,
  uniqExact(ts) AS buckets,
  min(ts) AS first_ts,
  max(ts) AS last_ts
FROM cachey.agg_15m
GROUP BY d
ORDER BY d DESC
LIMIT 5
"
```

Expected:
- `buckets = 96`
- `first_ts = 00:00:00`
- `last_ts = 23:45:00`

---

## Daily ingest (yesterday, 1-day delayed)

To keep load low and data stable, ingestion runs **once per day** for **yesterday (UTC)**.

### Idempotent daily ingest script (DELETE then INSERT)

Script: `scripts/ingest_yesterday_agg15m.sh`

Key properties:
- **1-day delayed**: ingests yesterday 00:00–23:45 UTC
- **Idempotent**: deletes that date’s rows before inserting
- **Concurrency safe**: uses `flock` lock to avoid overlap

Run manually:

```bash
./scripts/ingest_yesterday_agg15m.sh
```

### Cron (example)

```cron
SHELL=/bin/bash
PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

25 0 * * * /home/<user>/cdn-telemetry-kit/scripts/ingest_yesterday_agg15m.sh >> /home/<user>/cdn-telemetry-kit/local/cron_agg15m.log 2>&1
```

---

## Notes on performance & “VPS-friendly” operation

- `agg_15m` is intended for long-window graphs and “operational UI” queries.
- `raw_minute` is high fidelity and useful for ML feature generation; keeping **30 days** is feasible on modest VPS tiers depending on density/settings.
- ClickHouse TTL + partitions keep retention bounded over time (steady state after ~30 days).

---

## Public-safe security notes

- ClickHouse is often bound to `127.0.0.1` on the host; avoid exposing it publicly unless you add authentication + firewall rules.
- Avoid committing generated `__pycache__/` or `.venv/` directories:
  - `.gitignore` should include `__pycache__/`, `*.pyc`, `.venv/`

---

### Why “yesterday ingest” instead of “live streaming”?
It’s simpler, cheap on compute, and produces stable daily partitions suitable for demos, dashboards, and model training.

### Why keep both raw_minute and agg_15m?
- `raw_minute`: highest fidelity (ML + deep debug)
- `agg_15m`: fast long-range queries (graphs, UX, lower query cost)