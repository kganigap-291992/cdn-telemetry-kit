# Telemetry Contract v2


This document defines the shared telemetry contract and behavior model for the `cdn-telemetry-kit` generator used by both:

* Cachey
* ML anomaly-training repo

The goal of v2 is to preserve contract safety while making the synthetic telemetry behave much more like real CDN/video delivery traffic.

---

## 1. Non-negotiable constraints

### Shared source of truth

This generator is the shared source of truth for:

* Cachey
* ML anomaly-training repo

### Schema evolution rules

All schema evolution must be:

* additive-only
* contract-safe
* no renames
* no naming drift

### Time-series structure

The telemetry model keeps:

* `raw_minute` as the 1-minute source of truth
* `agg_15m` as the rollup derived from `raw_minute`

### Existing contract preservation

We keep:

* existing columns untouched
* CRC kept for now
* compatibility with both Cachey and the ML anomaly repo

---

## 2. Infra context

Cachey deployment context relevant to this contract:

* Cachey runs on Docker on a VPS
* ClickHouse is behind Caddy
* proxy access is only via `127.0.0.1`
* proxy server path is `/opt/cachey-proxy/server.js`

This document does not redefine the Cachey query contract, but it does define the telemetry behavior and emitted schema that Cachey depends on.

---

## 3. Purpose of v2

Telemetry Contract v1 produced structurally valid synthetic data, but it was not realistic enough for:

* convincing Cachey demos
* trustworthy deterministic triage narratives
* stronger anomaly-training inputs

Telemetry Contract v2 upgrades realism in these areas:

* traffic shape across the day
* weekend and event sensitivity
* sticky state behavior over time
* ATS result distribution realism
* content-aware latency behavior
* correlation across cache, errors, latency, and CRC
* exact and trustworthy 15-minute aggregation

The purpose is not to perfectly simulate a specific production environment. The purpose is to create a coherent, operationally believable CDN/video telemetry model that stays contract-safe.

---

## 4. Schema contract

### 4.1 Core raw minute shape

Each row in `raw_minute` represents:

* 1 minute
* 1 slice

Typical dimensions include:

* `seed`
* `ts`
* `partner`
* `service`
* `region`
* `pop`
* `host`
* `content_type`
* `ua_family`

Core metric families include:

* `requests`
* `bytes_sent`
* latency metrics (`p50_ms`, `p95_ms`, `p99_ms`)
* `cache_hit_rate`
* HTTP status aggregates
* status code detail columns
* `crc_errors`
* ATS result code columns

### 4.2 ATS columns added in v2

The following fields were added as additive-only columns:

* `ats_tcp_hit_count`
* `ats_tcp_cf_hit_count`
* `ats_tcp_miss_count`
* `ats_tcp_refresh_hit_count`
* `ats_tcp_ref_fail_hit_count`
* `ats_tcp_refresh_miss_count`
* `ats_tcp_client_refresh_count`
* `ats_tcp_ims_hit_count`
* `ats_tcp_ims_miss_count`
* `ats_tcp_swapfail_count`
* `ats_err_client_abort_count`
* `ats_err_client_read_error_count`
* `ats_err_connect_fail_count`
* `ats_err_dns_fail_count`
* `ats_err_invalid_req_count`
* `ats_err_read_timeout_count`
* `ats_err_proxy_denied_count`
* `ats_err_unknown_count`

### 4.3 Aggregated 15-minute shape

`agg_15m` is derived from `raw_minute`.

It preserves:

* exact-sum count-like metrics
* weighted/recomputed latency and cache metrics
* ATS result counts

It is not a separately modeled dataset.

---

## 5. Invariants

The generator preserves these invariants:

### Request accounting

* `http_2xx_count + http_3xx_count + http_4xx_count + http_5xx_count == requests`

### 2xx detail accounting

* `status_200 + status_206 == http_2xx_count`

### 3xx detail accounting

* `status_304 == http_3xx_count`

### 4xx detail accounting

* `status_403 + status_404 + status_429 == http_4xx_count`

### 5xx detail accounting

* `status_500 + status_502 + status_503 + status_504 == http_5xx_count`

### ATS accounting

* sum of all `ats_*` columns must equal `requests`

### Latency ordering

* `p50_ms <= p95_ms <= p99_ms`

---

## 6. Behavioral model

### 6.1 Traffic shaping

v2 replaces flatter synthetic traffic with a more realistic load profile:

* overnight trough
* morning ramp
* daytime plateau
* evening / prime-time peak
* weekend modifier
* deterministic event overlays

Service/content combinations such as `live`, `live_ott`, and `segment` are more sensitive to event windows.

This keeps the data aligned to video/CDN usage rather than generic web traffic.

### 6.2 Sticky minute-state engine

A service-level sticky state model drives temporal realism.

States:

* `healthy`
* `cache_pressure`
* `origin_slow`
* `network_issue`
* `bad_incident`

Characteristics:

* states persist across minutes
* transition risk increases during evening, weekends, and event windows
* live-oriented services are slightly more sensitive

These states influence:

* request behavior
* cache behavior
* latency
* 5xx pressure
* CRC behavior

### 6.3 ATS distribution realism

v2 introduces realistic ATS result-code generation.

The ATS mix is shaped by:

* content type
* minute state
* service
* cache pressure

Families represented:

* hit family
* miss family
* refresh / IMS family
* client issue family
* infra failure family
* rare issue family

Expected behavior:

* hit family dominates healthy traffic
* miss family grows under stress
* refresh / IMS behavior is more visible in manifest paths
* infra and client failures rise under degraded states

### 6.4 Content-aware latency

Latency is no longer generic. It responds to:

* ATS miss pressure
* refresh / IMS pressure
* client issue pressure
* infra failure pressure
* sticky minute state
* content type

Final intended ordering:

* `segment` highest latency sensitivity
* `manifest` medium
* `api` lowest / flattest

This preserves the identity of the generator as a video/CDN telemetry system.

### 6.5 Cache / error / CRC coherence

v2 makes rows internally coherent.

#### Cache hit rate

`cache_hit_rate` follows ATS hit/miss pressure and state effects.

#### 5xx behavior

`http_5xx_count` follows ATS infra/failure pressure plus state severity.

#### CRC behavior

`crc_errors` behave as a smaller correlated network/client stress signal.
CRC remains in the model for now.

---

## 7. Aggregation rules

`aggregate_logs()` rolls minute rows into `agg_15m`.

### 7.1 Exact-sum fields

The following categories are summed exactly:

* requests
* bytes
* aggregate status counts
* status detail counts
* CRC
* ATS counts

### 7.2 Weighted/recomputed fields

The following are request-weighted:

* `cache_hit_rate`
* `p50_ms`
* `p95_ms`
* `p99_ms`

### 7.3 Preserved guarantees

Aggregation preserves:

* request accounting invariants
* status detail consistency
* monotonic latency ordering (`p50 <= p95 <= p99`)

### 7.4 Important design rule

`agg_15m` is derived from `raw_minute` and must not drift into a separate behavioral model.

---

## 8. Cachey integration notes

### Current graphing behavior

As of this v2 work:

* Cachey uses `raw_minute` for windows under 6 hours
* Cachey uses `agg_15m` for windows greater than 6 hours

### Impact of v2

No rewiring is required just because of the v2 realism upgrade.

Why:

* the minute-level generator remains the source of truth
* `agg_15m` inherits changes through aggregation
* emitter scripts already use shared schema-order constants and shared generator functions

### Script compatibility

Current generator repo scripts already align to the shared contract:

* `emit_json_eachrow.py`
* `emit_agg15m_json_each_row.py`
* `gen_csv.py` (to be documented separately if needed)

The raw and aggregate emitters remain valid for Cachey ingestion/testing because they:

* import from the shared generator
* enforce canonical column ordering
* preserve ATS columns in emitted output

---

## 9. Phase-by-phase changelog

### Phase 1 — Schema Extension

Added ATS columns as additive-only fields.

### Phase 2 — Traffic Shape Upgrade

Introduced diurnal, weekend, commute, and event-driven traffic shaping.

### Phase 3 — Minute-State Engine

Added sticky service-level states that evolve over time.

### Phase 4 — ATS Distribution Logic

Introduced realistic ATS family/code distribution with content/state sensitivity.

### Phase 5 — Content-Aware Latency

Connected latency to ATS pressure, state, and content type.

### Phase 6 — Error / Cache / CRC Correlation

Made rows internally coherent by coupling cache, errors, and CRC to ATS/state behavior.

### Phase 7 — Aggregation Update

Extended aggregation to include ATS columns and preserve weighted metrics.

### Phase 8 — Validation Pass

Validated 24-hour shape, row-level coherence, ATS accounting, aggregation behavior, and content-type ordering.

Discovered one real bug during validation:

* content-type latency ordering was inverted

Applied a surgical fix so final intended latency ordering is:

* `segment > manifest > api`

### Phase 9 — Final Hardening

In progress.

Current sub-phases:

* Phase 9.1 multi-day validation
* Phase 9.2 stress-row review
* Phase 9.3 rollup confidence checks on extremes
* Phase 9.4 optional micro-tuning only if needed

---

## 10. Validation summary

### Phase 8 outcomes

Validated successfully:

* traffic has believable daily shape
* ATS family accounting remains exact
* cache/error/CRC correlation is coherent
* corrected content-type latency ordering holds
* raw minute and aggregate outputs both remain contract-safe
* raw and aggregate emitter scripts produce sane rows

### Phase 9 status

Phase 9.1 is underway to confirm long-window stability across 3-day and 7-day runs.

Interim results already showed:

* stable diurnal behavior over multiple days
* stable content-type ordering across raw and 15-minute views
* ATS mismatch remains zero

This section should be finalized after Phase 9 closes.

---

## 11. Known limitations

This is still synthetic telemetry.
It is designed to be operationally believable, not a perfect mirror of any one production system.

Current limitations include:

* simplified state model compared with real multi-layer CDN systems
* no direct representation yet of router/edge/mid/origin as separate linked layers
* CRC retained temporarily as a useful but simplified signal
* event overlays are deterministic rather than externally driven

These are acceptable for the current goals of Cachey realism and anomaly-training input quality.

---

## 12. Next steps after Phase 9

After Phase 9 closes:

1. finalize this document
2. commit the shared generator milestone
3. resume Cachey-side work only if a real integration issue appears
4. later extend the telemetry model for deeper layered delivery systems if needed

---

## 13. Working rules for future changes

Future changes to this generator must preserve the following:

* additive-only schema evolution
* no renames
* no naming drift
* `raw_minute` remains source of truth
* `agg_15m` remains derived
* minute/aggregate outputs stay compatible with both Cachey and ML anomaly training
* behavior should remain video/CDN-centric, not drift into generic web/app telemetry
