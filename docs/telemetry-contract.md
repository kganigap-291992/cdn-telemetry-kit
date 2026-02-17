# Telemetry Contract (Frozen)

This contract defines the canonical telemetry schema used by Cachey and the ML repo.

**Rule:** Never rename columns. Only add new columns (additive evolution).

## Layers

### 1) raw_minute (generator output)
Minute-level event rows. Highest fidelity.

Required columns (minimum):
- ts_minute (ISO or epoch minute)
- partner
- service            (live|vod)
- region
- pop
- path
- path_type          (manifest|segment|other)
- ua_family
- status_code
- cache_status       (hit|miss|pass)
- ttms_ms

Additive columns allowed:
- http_200_count (if represented as counts) OR derive via status_code=200

### 2) buckets_5m (aggregated for graphs)
Graph-ready 5-minute buckets.

Required columns (minimum):
- bucket_5m_start
- partner
- service
- region
- pop
- requests
- http_200_count
- http_4xx_count
- http_5xx_count
- cache_hit_count
- cache_miss_count
- p95_ttms_ms
- p99_ttms_ms

### 3) features_5m (ML frame)
Model-ready feature table derived from buckets_5m.

Required columns (minimum):
- bucket_5m_start
- partner
- service
- region
- pop
- (feature columns derived from buckets)

Note: Some ops fields may be excluded from features initially (documented here).

### 4) scores_zscore (ML outputs)
Outputs of anomaly detection.

Required columns (minimum):
- bucket_5m_start
- partner
- service
- region
- pop
- score_z
- is_anomaly
