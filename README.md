# CDN Telemetry Kit — Deterministic CDN Telemetry + ClickHouse Pipeline

This repo generates **deterministic, CDN-like telemetry** and provides a **ClickHouse ingestion pipeline** powering:

* 📊 **Cachey (deterministic triage + debugging)**
* 🤖 **Anomaly detection / ML training**
* 📈 **Operational dashboards (low-cost VPS-friendly)**

---

## 🚀 What makes this different

This is not random mock data.

It is:

* **Deterministic** → same seed = same data (reproducible debugging + ML)
* **Contract-safe** → schema never breaks downstream systems
* **CDN-realistic** → traffic, cache behavior, errors, latency distributions
* **Multi-layer telemetry** → raw + aggregated views
* **ATS-aware** → deep cache observability (hit/miss/error breakdown)

---

## 🧱 Architecture

```
VPS (private infra)
  ├─ cdn-telemetry-kit/
  │   ├─ src/telemetry_kit/       # generator + schema + aggregation
  │   └─ scripts/                 # emit + ingest
  ├─ .venv/
  └─ Docker
      └─ clickhouse-server (PRIVATE 127.0.0.1)
```

### Data Flow

```
Generator → JSONEachRow → ClickHouse (raw_minute)
                           ↓
                    aggregate_logs()
                           ↓
                     agg_15m table
```

* Insert via:

```bash
python emit_*.py | clickhouse-client INSERT FORMAT JSONEachRow
```

---

## 🗂️ Data Layers

### 1) `raw_minute` (high fidelity)

* 1 row = **1 minute × 1 slice**
* Used for:

  * ML training
  * deep debugging
  * anomaly signals

### 2) `agg_15m` (operational layer)

* 15-minute buckets
* Used for:

  * dashboards
  * Cachey UI
  * long-window queries

---

## 🧠 Slice Dimensions

* `partner`, `service`
* `region`, `pop`, `host`
* `content_type`, `ua_family`

---

## 📊 Metrics

### Core

* `requests`, `bytes_sent`
* `p50_ms`, `p95_ms`, `p99_ms`
* `cache_hit_rate`
* `crc_errors`

### HTTP

* `http_2xx/3xx/4xx/5xx`
* detailed: `status_200, 206, 304, 403, 404, 429, 500, 502, 503, 504`

---

## 🧩 NEW: ATS Telemetry (Cache Observability)

Fully integrated ATS-style counters:

* hits: `ats_tcp_hit_count`, `ats_tcp_cf_hit_count`
* misses: `ats_tcp_miss_count`, `ats_tcp_refresh_miss_count`
* revalidation: `ims_hit/miss`, `refresh_hit`
* failures: `swapfail`, `ref_fail`
* client behavior: `client_refresh`
* errors: DNS, timeout, connect, abort, etc.

### 🔒 Invariant

```
sum(all ATS counters) == requests
```

This ensures:

* no data drift
* consistent accounting
* reliable ML features

---

## 📐 Generator Guarantees (Invariants)

* HTTP buckets sum exactly to requests
* Status codes match buckets
* ATS totals match requests
* p50 ≤ p95 ≤ p99
* raw and agg are mathematically consistent

---

## 🔁 Raw ↔ Aggregate Consistency (IMPORTANT)

Recent fix ensures:

✅ `agg_15m` is generated from **full-window deterministic aggregation**
❌ no chunk-based drift

### Guarantee

For every bucket:

```
raw_rollup == agg_15m
```

Validated via:

* request parity
* ATS parity
* zero diff across buckets

---

## 🧊 Schema Contract (FROZEN)

* ❌ Never rename columns
* ❌ Never reorder columns
* ✅ Only additive changes allowed

This protects:

* Cachey
* ML pipelines
* SQL contracts

---

## ⏳ Retention (TTL)

Both tables:

```
TTL ts + toIntervalDay(30)
```

* automatic cleanup
* bounded storage
* no manual deletes needed

---

## ⚙️ Daily Ingestion (Production Pattern)

### Why not streaming?

* cheaper
* deterministic
* stable partitions

### Flow

```
00:10 UTC → seed raw (yesterday)
00:25 UTC → build agg_15m (yesterday)
```

---

### Raw Ingest

Script: `seed_yesterday.sh`

* deletes yesterday
* regenerates deterministically
* inserts into `raw_minute`
* validates row count + timestamps

---

### Aggregate Ingest

Script: `ingest_yesterday_agg15m.sh`

* deletes yesterday
* regenerates aggregates
* inserts into `agg_15m`
* protected by `flock` (no overlap)

---

## 🔁 Idempotency

Daily ingest is:

```
DELETE → INSERT
```

So:

* safe to rerun
* no duplicates
* consistent results

---

## 🧪 Verification

### Check TTL + schema

```bash
SHOW CREATE TABLE cachey.raw_minute
```

### Check bucket correctness

```sql
SELECT
  ts,
  sum(requests)
FROM cachey.agg_15m
GROUP BY ts
```

### Check parity (raw vs agg)

```sql
-- raw rollup vs agg comparison
```

---

## 🔐 Security

* ClickHouse bound to `127.0.0.1`
* accessed via proxy (not public)
* firewall + fail2ban recommended

---

## 🧠 Why this matters (Cachey + ML)

This repo is the **data backbone** for:

### Cachey

* deterministic triage
* SQL-backed evidence
* no hallucination debugging

### ML / Anomaly Detection

* reproducible training data
* controlled incident simulation
* feature-rich telemetry (ATS + latency + errors)

---

## 🧭 Design Philosophy

* Deterministic > Random
* Evidence > Guessing
* Additive evolution only
* One source of truth (generator)
* Raw + Aggregated dual-layer design

---

## 🛣️ Roadmap

* traffic realism (diurnal, events)
* state engine (healthy / degraded / incident)
* ATS-aware anomaly detection
* ClickHouse → UI integration (Cachey)
* LLM explanation layer (on top of deterministic data)

---

## 🧼 Notes

* ignore `.venv/`, `__pycache__/`
* use UTC everywhere
* keep generator contract stable

---

## 💡 Summary

This is a **deterministic CDN telemetry engine** with:

* real-world cache signals (ATS)
* reproducible data generation
* ClickHouse-optimized ingestion
* production-like daily pipelines

Built to power:

* debugging systems (Cachey)
* ML pipelines
* observability platforms
