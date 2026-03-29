# Telemetry Contract v2

This document defines the shared telemetry contract and behavior model for the `cdn-telemetry-kit` generator used by both:

* Cachey
* ML anomaly-training repo

The goal of v2 is to preserve contract safety while making synthetic telemetry behave like real CDN/video delivery systems.

---

## 1. Non-negotiable constraints

### Shared source of truth

This generator is the single source of truth for:

* Cachey
* ML anomaly-training repo

---

### Schema evolution rules

All schema evolution must be:

* additive-only
* contract-safe
* no renames
* no naming drift

---

### Time-series structure

* `raw_minute` → source of truth
* `agg_15m` → derived rollup

---

### Existing contract preservation

We preserve:

* all existing columns
* CRC (temporary signal)
* compatibility with Cachey + ML pipelines

---

## 2. Infra context

* Cachey runs on Docker (VPS)
* ClickHouse is behind Caddy
* access restricted to `127.0.0.1`
* proxy path: `/opt/cachey-proxy/server.js`

---

## 3. Purpose of v2

v1 produced structurally valid data, but lacked realism.

v2 improves:

* traffic shape (diurnal + events)
* sticky state behavior
* ATS distribution realism
* content-aware latency
* cache/error/CRC correlation
* aggregation correctness

Goal:

> Operationally believable CDN telemetry that remains contract-safe.

---

## 4. Schema contract

### 4.1 Raw minute shape

Each row:

* 1 minute
* 1 slice

Dimensions:

* `partner`, `service`, `region`, `pop`, `host`, `content_type`, `ua_family`

Metrics:

* requests, bytes
* latency
* cache
* HTTP breakdown
* CRC
* ATS

---

### 4.2 ATS columns (v2)

Additive-only:

* hits, misses, refresh, IMS
* client signals
* infra failures
* rare error buckets

---

### 4.3 Aggregated shape (`agg_15m`)

Derived from raw:

* exact-sum counters
* weighted latency + cache
* ATS preserved

---

## 5. Invariants

### Request accounting

```
http_2xx + http_3xx + http_4xx + http_5xx == requests
```

### Status detail

* 200 + 206 = 2xx
* 304 = 3xx
* 403 + 404 + 429 = 4xx
* 500 + 502 + 503 + 504 = 5xx

### ATS accounting

```
sum(all ats_*) == requests
```

### Latency ordering

```
p50 <= p95 <= p99
```

---

## 6. Behavioral model

### Traffic shaping

* diurnal
* weekend
* event overlays

---

### Sticky state engine

States:

* healthy
* cache_pressure
* origin_slow
* network_issue
* bad_incident

Affects:

* latency
* errors
* cache
* ATS mix

---

### ATS realism

Families:

* hit
* miss
* refresh / IMS
* client issues
* infra failures

---

### Content-aware latency

Ordering:

```
segment > manifest > api
```

---

### Coherence model

* cache_hit_rate aligns with ATS
* 5xx aligns with infra failure
* CRC aligns with stress

---

## 7. Aggregation rules

### Exact-sum

* requests
* status counts
* ATS
* CRC

### Weighted

* latency
* cache_hit_rate

---

### 🔴 Critical design rule

`agg_15m` must NEVER diverge from raw behavior.

---

## 8. 🔥 Production alignment (Phase 10)

Recent work validated real system behavior:

### 8.1 ATS plumbing (completed)

* ATS added to raw + agg schemas
* deterministic ATS generation implemented
* ATS sum == requests verified

---

### 8.2 Aggregation correctness fix (critical)

A real bug was identified:

* chunked agg generation caused drift vs raw

Fix:

* full-window deterministic aggregation
* agg now matches raw exactly

---

### 8.3 Raw ↔ Aggregate parity (validated)

For every bucket:

```
raw_rollup == agg_15m
```

Validated across:

* requests
* ATS
* zero-diff buckets

---

### 8.4 Controlled backfill

* recent windows deleted + regenerated
* validated consistency post-fix

---

### 8.5 Cron + TTL stability

* daily ingestion verified
* delete → insert pattern preserved
* TTL confirmed (30 days)

---

## 9. Cachey integration

* < 6h → raw_minute
* > 6h → agg_15m

No contract changes required.

---

## 10. Phase status

### Completed

* Phase 1–7: core model
* Phase 8: validation
* Phase 9: hardening
* Phase 10:

  * ATS plumbing ✅
  * agg fix ✅
  * backfill ✅
  * cron + TTL verification ✅

---

## 11. Known limitations

* simplified CDN layering (no edge/mid/origin split yet)
* CRC is simplified signal
* deterministic events (not external)

---

## 12. Next steps

* layered CDN modeling (future)
* anomaly feature tuning
* Cachey integration polish
* LLM explanation layer

---

## 13. Working rules

* additive-only schema
* no renames
* no drift
* raw = source of truth
* agg = derived
* remain CDN/video-centric
