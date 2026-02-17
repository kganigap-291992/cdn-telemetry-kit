from __future__ import annotations

# ------------------------------------------------------------
# Canonical allowed values (additive only)
# ------------------------------------------------------------
DEFAULT_SERVICES = ["live", "vod", "dvr", "eas", "live_ott", "app_backend"]
DEFAULT_CONTENT_TYPES = ["manifest", "segment", "api"]
DEFAULT_REGIONS = [
    "us-east", "us-west", "us-central",
    "eu-west", "eu-central",
    "ap-south", "ap-northeast",
    "sa-east",
]
DEFAULT_UA_FAMILIES = ["stb", "mobile", "web", "smart_tv", "console"]

# ------------------------------------------------------------
# Stable column order for raw_minute CSV output
# Rule: never rename; never reorder existing; only ADD new columns.
# ------------------------------------------------------------
RAW_MINUTE_COLUMNS = [
    # Provenance (metadata)
    "seed",

    # Slice dimensions
    "ts",
    "partner",
    "service",
    "region",
    "pop",
    "host",
    "content_type",
    "ua_family",

    # Core metrics
    "requests",
    "bytes_sent",
    "p50_ms",
    "p95_ms",
    "p99_ms",
    "cache_hit_rate",

    # Status buckets (must sum to requests)
    "http_2xx_count",
    "http_3xx_count",
    "http_4xx_count",
    "http_5xx_count",

    # Detailed 5xx breakdown (subset of http_5xx_count)
    "status_500",
    "status_502",
    "status_503",
    "status_504",

    # Other signals
    "crc_errors",
]
