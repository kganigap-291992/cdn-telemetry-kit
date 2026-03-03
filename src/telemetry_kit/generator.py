from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import List, Tuple

import numpy as np
import pandas as pd

from .schema import (
    DEFAULT_SERVICES,
    DEFAULT_REGIONS,
    DEFAULT_CONTENT_TYPES,
    DEFAULT_UA_FAMILIES,
)


# -----------------------------
# Incident model
# -----------------------------
@dataclass
class Incident:
    """Incident applied to a subset of slices for a time window."""
    name: str
    start_ts: datetime
    end_ts: datetime
    kind: str  # latency | cache_collapse | origin_overload | timeouts | crc_spike
    partner: str | None = None
    service: str | None = None
    region: str | None = None
    pop: str | None = None
    content_type: str | None = None
    intensity: float = 1.0


# -----------------------------
# Helpers
# -----------------------------
def _ensure_utc(dt: datetime) -> datetime:
    """Accept naive as UTC; otherwise convert to UTC."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _utc_minute_floor(dt: datetime) -> datetime:
    dt = _ensure_utc(dt)
    return dt.replace(second=0, microsecond=0)


def _utc_bucket_floor(dt: datetime, bucket_minutes: int) -> datetime:
    """
    Floor dt to the start of its UTC bucket.
    Example: 12:07 with bucket=15 => 12:00, 12:29 => 12:15.
    """
    if bucket_minutes <= 0:
        raise ValueError("bucket_minutes must be > 0")
    dt = _ensure_utc(dt).replace(second=0, microsecond=0)
    m = (dt.minute // bucket_minutes) * bucket_minutes
    return dt.replace(minute=m)


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _matches(val: str, expected: str | None) -> bool:
    return True if expected is None else (val == expected)


# -----------------------------
# Aggregation (minute -> N-minute)
# -----------------------------
def aggregate_logs(df: pd.DataFrame, bucket_minutes: int = 15) -> pd.DataFrame:
    """
    Roll up minute-level aggregated rows into bucket_minutes buckets.

    Principles:
    - Sum all count-like metrics exactly (requests, bytes, status buckets, crc_errors).
    - Compute request-weighted averages for:
        - cache_hit_rate
        - p50_ms / p95_ms / p99_ms
      (Deterministic + stable; good for demo and ML features later.)
    - Preserve invariants:
        - http_2xx+http_3xx+http_4xx+http_5xx == requests (fix drift into 2xx)
        - p50 <= p95 <= p99 (force ordering)
    """
    if df is None or df.empty:
        return pd.DataFrame()

    if bucket_minutes <= 0:
        raise ValueError("bucket_minutes must be > 0")

    d = df.copy()

    # Ensure ts is UTC datetime and floored to bucket start
    d["ts"] = pd.to_datetime(d["ts"], utc=True)
    d["ts"] = d["ts"].apply(lambda x: _utc_bucket_floor(x.to_pydatetime(), bucket_minutes))

    keys = [
        "seed",
        "ts",
        "partner",
        "service",
        "region",
        "pop",
        "host",
        "content_type",
        "ua_family",
    ]

    # Sum-exact metrics
    sum_cols = [
        "requests",
        "bytes_sent",
        "http_2xx_count",
        "http_3xx_count",
        "http_4xx_count",
        "http_5xx_count",
        "status_200",
        "status_206",
        "status_304",
        "status_403",
        "status_404",
        "status_429",
        "status_500",
        "status_502",
        "status_503",
        "status_504",
        "crc_errors",
    ]

    # Weighted metrics (by requests)
    def _wmean(g: pd.DataFrame, col: str) -> float:
        w = g["requests"].to_numpy(dtype=float)
        x = g[col].to_numpy(dtype=float)
        sw = float(w.sum())
        if sw <= 0.0:
            return 0.0
        return float((w * x).sum() / sw)

    grouped = d.groupby(keys, as_index=False)

    out_sum = grouped[sum_cols].sum()

    # Compute weighted metrics per group
    weighted_rows = []
    for _, g in grouped:
        weighted_rows.append(
            {
                "seed": int(g["seed"].iloc[0]),
                "ts": g["ts"].iloc[0],
                "partner": g["partner"].iloc[0],
                "service": g["service"].iloc[0],
                "region": g["region"].iloc[0],
                "pop": g["pop"].iloc[0],
                "host": g["host"].iloc[0],
                "content_type": g["content_type"].iloc[0],
                "ua_family": g["ua_family"].iloc[0],
                "p50_ms": _wmean(g, "p50_ms"),
                "p95_ms": _wmean(g, "p95_ms"),
                "p99_ms": _wmean(g, "p99_ms"),
                "cache_hit_rate": _wmean(g, "cache_hit_rate"),
            }
        )
    out_w = pd.DataFrame(weighted_rows)

    # Merge summed + weighted
    out = out_sum.merge(out_w, on=keys, how="left")

    if out.empty:
        return out

    # Re-assert bucket sum invariant (absorb drift into 2xx)
    bucket_sum = (
        out["http_2xx_count"]
        + out["http_3xx_count"]
        + out["http_4xx_count"]
        + out["http_5xx_count"]
    )
    drift = (out["requests"] - bucket_sum).astype(int)
    if (drift != 0).any():
        out.loc[drift != 0, "http_2xx_count"] = (
            out.loc[drift != 0, "http_2xx_count"] + drift[drift != 0]
        ).clip(lower=0)

        # keep 2xx detailed counts consistent with 2xx bucket if needed:
        # status_200 + status_206 should match http_2xx_count.
        two_xx_detail = out["status_200"] + out["status_206"]
        drift2 = (out["http_2xx_count"] - two_xx_detail).astype(int)
        if (drift2 != 0).any():
            # absorb into status_200 (safe choice)
            out.loc[drift2 != 0, "status_200"] = (out.loc[drift2 != 0, "status_200"] + drift2[drift2 != 0]).clip(lower=0)

    # Enforce percentile ordering
    out["p95_ms"] = out[["p95_ms", "p50_ms"]].max(axis=1)
    out["p99_ms"] = out[["p99_ms", "p95_ms"]].max(axis=1)

    return out


# -----------------------------
# Generator
# -----------------------------
def generate_minute_logs(
    start_ts_utc: datetime,
    minutes: int,
    n_partners: int = 6,
    n_pops: int = 20,
    n_hosts: int = 120,
    services: List[str] | None = None,
    regions: List[str] | None = None,
    content_types: List[str] | None = None,
    ua_families: List[str] | None = None,
    seed: int = 7,
    incidents: List[Incident] | None = None,
    density: float = 0.10,
) -> pd.DataFrame:
    """
    Generate minute-level aggregated CDN-like telemetry.
    Each row = 1 minute × 1 slice.
    Returns a DataFrame (no file I/O here).

    Guarantees:
    - http_2xx + http_3xx + http_4xx + http_5xx == requests
    - status_200 + status_206 == http_2xx
    - status_304 == http_3xx
    - status_403 + status_404 + status_429 == http_4xx
    - status_500 + status_502 + status_503 + status_504 == http_5xx
    """
    rng = np.random.default_rng(seed)

    services = services or DEFAULT_SERVICES
    regions = regions or DEFAULT_REGIONS
    content_types = content_types or DEFAULT_CONTENT_TYPES
    ua_families = ua_families or DEFAULT_UA_FAMILIES
    incidents = incidents or []

    ts0 = _utc_minute_floor(start_ts_utc)

    partners = [f"partner_{i:02d}" for i in range(1, n_partners + 1)]
    pops = [f"pop_{i:03d}" for i in range(1, n_pops + 1)]
    hosts = [f"host_{i:04d}" for i in range(1, n_hosts + 1)]

    # Build a pool of plausible slices
    slice_pool: List[Tuple[str, str, str, str, str, str, str]] = []
    for _ in range(5000):
        slice_pool.append(
            (
                rng.choice(partners),
                rng.choice(services),
                rng.choice(regions),
                rng.choice(pops),
                rng.choice(hosts),
                rng.choice(content_types),
                rng.choice(ua_families),
            )
        )

    def traffic_multiplier(ts: datetime) -> float:
        hour = ts.hour
        return 0.85 + 0.35 * math.sin((hour - 14) * (2 * math.pi / 24))

    rows = []

    for m in range(minutes):
        ts = ts0 + timedelta(minutes=m)
        mult = traffic_multiplier(ts)

        k = max(50, int(len(slice_pool) * density))
        idxs = rng.choice(len(slice_pool), size=k, replace=False)

        for idx in idxs:
            partner, service, region, pop, host, ctype, ua_family = slice_pool[idx]

            base_rps = {
                "live": 90,
                "vod": 60,
                "dvr": 25,
                "eas": 10,
                "live_ott": 40,
                "app_backend": 35,
            }.get(service, 30)

            ctype_mult = {"manifest": 0.35, "segment": 1.0, "api": 0.55}.get(ctype, 0.6)
            region_mult = 1.0 + (0.15 if region.startswith("us") else 0.05)

            lam = max(0.0, base_rps * 60 * ctype_mult * region_mult * mult)
            requests = int(rng.poisson(lam=lam))
            if requests == 0:
                continue

            base_cache = {"manifest": 0.82, "segment": 0.90, "api": 0.55}.get(ctype, 0.75)
            cache_hit = float(_clamp(rng.normal(base_cache, 0.05), 0.05, 0.99))

            base_p50 = {"manifest": 120, "segment": 80, "api": 160}.get(ctype, 110)
            svc_add = {
                "live": 15,
                "vod": 10,
                "dvr": 20,
                "eas": 25,
                "live_ott": 18,
                "app_backend": 30,
            }.get(service, 15)

            p50 = max(5.0, rng.lognormal(mean=math.log(base_p50 + svc_add), sigma=0.25))
            p95 = p50 * float(rng.normal(2.2, 0.25))
            p99 = p50 * float(rng.normal(3.4, 0.35))

            avg_bytes = {"manifest": 18_000, "segment": 900_000, "api": 45_000}.get(ctype, 120_000)
            bytes_sent = int(requests * max(2000.0, rng.normal(avg_bytes, avg_bytes * 0.15)))

            # --- Explicit 5xx ---
            base_500, base_502, base_503, base_504 = 0.0004, 0.0003, 0.0002, 0.0002
            if service == "app_backend":
                base_500 *= 2.0
                base_504 *= 1.5

            status_500 = int(rng.binomial(requests, base_500))
            status_502 = int(rng.binomial(requests, base_502))
            status_503 = int(rng.binomial(requests, base_503))
            status_504 = int(rng.binomial(requests, base_504))

            mb = bytes_sent / 1e6
            crc_errors = int(rng.poisson(lam=max(0.0, mb * 0.002)))

            # --- Incidents modify metrics ---
            for inc in incidents:
                if not (inc.start_ts <= ts < inc.end_ts):
                    continue
                if not (
                    _matches(partner, inc.partner)
                    and _matches(service, inc.service)
                    and _matches(region, inc.region)
                    and _matches(pop, inc.pop)
                    and _matches(ctype, inc.content_type)
                ):
                    continue

                inten = max(0.1, inc.intensity)

                if inc.kind == "latency":
                    p50 *= 1.3 * inten
                    p95 *= 1.8 * inten
                    p99 *= 2.2 * inten
                elif inc.kind == "cache_collapse":
                    cache_hit = _clamp(cache_hit - (0.35 * inten), 0.01, 0.99)
                    p95 *= 1.4 * inten
                    p99 *= 1.7 * inten
                elif inc.kind == "origin_overload":
                    status_503 += int(requests * _clamp(0.02 * inten, 0.0, 0.4))
                    p95 *= 1.5 * inten
                    p99 *= 1.9 * inten
                elif inc.kind == "timeouts":
                    status_504 += int(requests * _clamp(0.015 * inten, 0.0, 0.35))
                    p99 *= 2.4 * inten
                elif inc.kind == "crc_spike":
                    crc_errors += int(max(0.0, mb * (0.25 * inten)))

            # Ensure ordering sanity
            p95 = max(p95, p50)
            p99 = max(p99, p95)

            # -----------------------------
            # Bucketed status counts
            # -----------------------------
            http_5xx = status_500 + status_502 + status_503 + status_504
            remaining = max(0, requests - http_5xx)

            # 4xx baseline: slightly higher for app_backend + api
            base_4xx = 0.004
            if service == "app_backend":
                base_4xx *= 2.0
            if ctype == "api":
                base_4xx *= 1.5

            http_4xx = int(rng.binomial(remaining, min(base_4xx, 0.25)))
            remaining -= http_4xx

            # 3xx baseline: redirects more likely on manifests/api
            base_3xx = 0.02
            if ctype == "manifest":
                base_3xx *= 1.3
            if ctype == "api":
                base_3xx *= 1.1

            http_3xx = int(rng.binomial(remaining, min(base_3xx, 0.40)))
            remaining -= http_3xx

            http_2xx = max(0, remaining)

            # Final guarantee for bucket sum
            if (http_2xx + http_3xx + http_4xx + http_5xx) != requests:
                http_2xx = max(0, requests - (http_3xx + http_4xx + http_5xx))

            # -----------------------------
            # Detailed status breakdown
            # -----------------------------
            # 2xx: split 200 vs 206 (segments => mostly 206)
            if http_2xx > 0:
                if ctype == "segment":
                    status_206 = int(rng.binomial(http_2xx, 0.90))
                    status_200 = http_2xx - status_206
                else:
                    status_200 = int(rng.binomial(http_2xx, 0.85))
                    status_206 = http_2xx - status_200
            else:
                status_200 = 0
                status_206 = 0

            # 3xx: model all as 304 for now
            status_304 = http_3xx

            # 4xx: split 403/404/429
            if http_4xx > 0:
                status_404 = int(rng.binomial(http_4xx, 0.50))
                rem4 = http_4xx - status_404

                status_403 = int(rng.binomial(rem4, 0.30))
                rem4 -= status_403

                status_429 = rem4
            else:
                status_403 = 0
                status_404 = 0
                status_429 = 0

            # (Optional paranoia checks in dev; keep cheap)
            if (status_200 + status_206) != http_2xx:
                status_200 = max(0, http_2xx - status_206)
            if status_304 != http_3xx:
                status_304 = http_3xx
            if (status_403 + status_404 + status_429) != http_4xx:
                # Adjust 429 to absorb drift
                status_429 = max(0, http_4xx - (status_403 + status_404))

            rows.append(
                {
                    "seed": int(seed),
                    "ts": ts,
                    "partner": partner,
                    "service": service,
                    "region": region,
                    "pop": pop,
                    "host": host,
                    "content_type": ctype,
                    "ua_family": ua_family,
                    "requests": requests,
                    "bytes_sent": bytes_sent,
                    "p50_ms": float(p50),
                    "p95_ms": float(p95),
                    "p99_ms": float(p99),
                    "cache_hit_rate": float(cache_hit),

                    # Buckets
                    "http_2xx_count": int(http_2xx),
                    "http_3xx_count": int(http_3xx),
                    "http_4xx_count": int(http_4xx),
                    "http_5xx_count": int(http_5xx),

                    # Detailed 2xx / 3xx / 4xx
                    "status_200": int(status_200),
                    "status_206": int(status_206),
                    "status_304": int(status_304),
                    "status_403": int(status_403),
                    "status_404": int(status_404),
                    "status_429": int(status_429),

                    # Detailed 5xx
                    "status_500": status_500,
                    "status_502": status_502,
                    "status_503": status_503,
                    "status_504": status_504,

                    "crc_errors": crc_errors,
                }
            )

    df = pd.DataFrame(rows)
    if not df.empty:
        df["ts"] = pd.to_datetime(df["ts"], utc=True)
    return df