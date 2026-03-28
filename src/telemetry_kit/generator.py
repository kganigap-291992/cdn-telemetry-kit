from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd

from .schema import (
    DEFAULT_SERVICES,
    DEFAULT_REGIONS,
    DEFAULT_CONTENT_TYPES,
    DEFAULT_UA_FAMILIES,
)


ATS_COLUMNS = [
    "ats_tcp_hit_count",
    "ats_tcp_cf_hit_count",
    "ats_tcp_miss_count",
    "ats_tcp_refresh_hit_count",
    "ats_tcp_ref_fail_hit_count",
    "ats_tcp_refresh_miss_count",
    "ats_tcp_client_refresh_count",
    "ats_tcp_ims_hit_count",
    "ats_tcp_ims_miss_count",
    "ats_tcp_swapfail_count",
    "ats_err_client_abort_count",
    "ats_err_client_read_error_count",
    "ats_err_connect_fail_count",
    "ats_err_dns_fail_count",
    "ats_err_invalid_req_count",
    "ats_err_read_timeout_count",
    "ats_err_proxy_denied_count",
    "ats_err_unknown_count",
]


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
    - Sum all count-like metrics exactly:
      requests, bytes, status buckets, crc_errors, ATS counts.
    - Compute request-weighted averages for:
        - cache_hit_rate
        - p50_ms / p95_ms / p99_ms
    - Preserve invariants:
        - http_2xx+http_3xx+http_4xx+http_5xx == requests
        - p50 <= p95 <= p99
    """
    if df is None or df.empty:
        return pd.DataFrame()

    if bucket_minutes <= 0:
        raise ValueError("bucket_minutes must be > 0")

    d = df.copy()

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
        *ATS_COLUMNS,
    ]

    def _wmean(g: pd.DataFrame, col: str) -> float:
        w = g["requests"].to_numpy(dtype=float)
        x = g[col].to_numpy(dtype=float)
        sw = float(w.sum())
        if sw <= 0.0:
            return 0.0
        return float((w * x).sum() / sw)

    grouped = d.groupby(keys, as_index=False)
    out_sum = grouped[sum_cols].sum()

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

    out = out_sum.merge(out_w, on=keys, how="left")

    if out.empty:
        return out

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

        two_xx_detail = out["status_200"] + out["status_206"]
        drift2 = (out["http_2xx_count"] - two_xx_detail).astype(int)
        if (drift2 != 0).any():
            out.loc[drift2 != 0, "status_200"] = (
                out.loc[drift2 != 0, "status_200"] + drift2[drift2 != 0]
            ).clip(lower=0)

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

    Phase 7 scope:
    - keep realistic traffic/state/ATS/latency/correlation logic
    - update aggregation support for ATS columns
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

    # -----------------------------
    # Phase 2 traffic shaping helpers
    # -----------------------------
    def _hourly_base_multiplier(hour: int) -> float:
        if 0 <= hour < 6:
            return 0.42
        if 6 <= hour < 9:
            return 0.55 + (hour - 6) * 0.12
        if 9 <= hour < 16:
            return 0.95
        if 16 <= hour < 19:
            return 1.10 + (hour - 16) * 0.12
        if 19 <= hour < 22:
            return 1.45
        return 0.92

    def _weekend_modifier(ts: datetime) -> float:
        return 1.12 if ts.weekday() >= 5 else 1.00

    def _commute_modifier(ts: datetime, service: str, ctype: str) -> float:
        if 7 <= ts.hour < 9:
            if service in {"live", "live_ott", "vod"} and ctype == "manifest":
                return 1.08
        return 1.00

    def _event_overlay(ts: datetime, service: str, ctype: str) -> float:
        dow = ts.weekday()
        hour = ts.hour

        event_strength = 0.0

        if dow in {2, 4} and 19 <= hour < 22:
            event_strength = 0.20

        if dow in {5, 6} and 18 <= hour < 22:
            event_strength = max(event_strength, 0.35)

        if dow == 6 and 13 <= hour < 17:
            event_strength = max(event_strength, 0.18)

        if event_strength <= 0.0:
            return 1.0

        service_sensitivity = {
            "live": 1.35,
            "live_ott": 1.28,
            "vod": 1.05,
            "dvr": 1.00,
            "eas": 0.95,
            "app_backend": 0.88,
        }.get(service, 1.0)

        content_sensitivity = {
            "segment": 1.25,
            "manifest": 1.08,
            "api": 0.82,
        }.get(ctype, 1.0)

        return 1.0 + (event_strength * service_sensitivity * content_sensitivity)

    def traffic_multiplier(ts: datetime, service: str, ctype: str) -> float:
        base = _hourly_base_multiplier(ts.hour)
        weekend = _weekend_modifier(ts)
        commute = _commute_modifier(ts, service, ctype)
        event = _event_overlay(ts, service, ctype)
        return base * weekend * commute * event

    # -----------------------------
    # Phase 3 sticky minute-state engine
    # -----------------------------
    STATES = [
        "healthy",
        "cache_pressure",
        "origin_slow",
        "network_issue",
        "bad_incident",
    ]

    def _state_transition_probs(current_state: str, ts: datetime, service: str) -> list[float]:
        hour = ts.hour
        dow = ts.weekday()

        prime_risk = 0.0
        if 18 <= hour < 22:
            prime_risk += 0.015
        if dow in {5, 6} and 18 <= hour < 22:
            prime_risk += 0.015
        if dow == 6 and 13 <= hour < 17:
            prime_risk += 0.010

        service_risk = {
            "live": 1.25,
            "live_ott": 1.18,
            "vod": 1.00,
            "dvr": 0.95,
            "eas": 0.90,
            "app_backend": 1.05,
        }.get(service, 1.0)

        risk = prime_risk * service_risk

        if current_state == "healthy":
            p_healthy = max(0.88, 0.965 - risk)
            p_cache = 0.012 + (risk * 0.50)
            p_origin = 0.009 + (risk * 0.35)
            p_network = 0.008 + (risk * 0.25)
            p_bad = 0.006 + (risk * 0.10)
            probs = [p_healthy, p_cache, p_origin, p_network, p_bad]
        elif current_state == "cache_pressure":
            probs = [0.14, 0.76, 0.05, 0.03, 0.02]
        elif current_state == "origin_slow":
            probs = [0.12, 0.05, 0.74, 0.03, 0.06]
        elif current_state == "network_issue":
            probs = [0.13, 0.03, 0.03, 0.72, 0.09]
        else:
            probs = [0.10, 0.05, 0.08, 0.07, 0.70]

        total = sum(probs)
        return [p / total for p in probs]

    def _build_service_state_timelines() -> dict[str, list[str]]:
        timelines: dict[str, list[str]] = {}
        for service in services:
            current_state = "healthy"
            service_states: list[str] = []

            for minute_idx in range(minutes):
                ts = ts0 + timedelta(minutes=minute_idx)
                probs = _state_transition_probs(current_state, ts, service)
                current_state = str(rng.choice(STATES, p=probs))
                service_states.append(current_state)

            timelines[service] = service_states
        return timelines

    service_state_timelines = _build_service_state_timelines()

    def _state_request_multiplier(state: str) -> float:
        return {
            "healthy": 1.00,
            "cache_pressure": 0.98,
            "origin_slow": 0.99,
            "network_issue": 0.96,
            "bad_incident": 0.93,
        }[state]

    def _state_cache_delta(state: str) -> float:
        return {
            "healthy": 0.00,
            "cache_pressure": -0.08,
            "origin_slow": -0.03,
            "network_issue": -0.02,
            "bad_incident": -0.15,
        }[state]

    def _state_latency_multipliers(state: str) -> tuple[float, float, float]:
        return {
            "healthy": (1.00, 1.00, 1.00),
            "cache_pressure": (1.05, 1.12, 1.20),
            "origin_slow": (1.12, 1.30, 1.45),
            "network_issue": (1.08, 1.24, 1.52),
            "bad_incident": (1.28, 1.62, 2.00),
        }[state]

    # -----------------------------
    # Phase 4 ATS distribution
    # -----------------------------
    def _ats_family_targets(
        state: str,
        ctype: str,
        service: str,
        cache_hit: float,
    ) -> Dict[str, float]:
        if ctype == "segment":
            hit_family = _clamp(cache_hit, 0.74, 0.90)
            miss_family = 0.11
            refresh_ims_family = 0.035
            client_issue_family = 0.020
            infra_fail_family = 0.004
            rare_family = 0.001
        elif ctype == "manifest":
            hit_family = _clamp(cache_hit, 0.70, 0.86)
            miss_family = 0.09
            refresh_ims_family = 0.080
            client_issue_family = 0.020
            infra_fail_family = 0.008
            rare_family = 0.002
        else:
            hit_family = _clamp(cache_hit, 0.52, 0.70)
            miss_family = 0.22
            refresh_ims_family = 0.035
            client_issue_family = 0.025
            infra_fail_family = 0.015
            rare_family = 0.005

        base_non_hit = (
            miss_family
            + refresh_ims_family
            + client_issue_family
            + infra_fail_family
            + rare_family
        )
        residual = max(0.0, 1.0 - hit_family)
        scale = residual / base_non_hit if base_non_hit > 0 else 1.0

        miss_family *= scale
        refresh_ims_family *= scale
        client_issue_family *= scale
        infra_fail_family *= scale
        rare_family *= scale

        if state == "cache_pressure":
            hit_family -= 0.10
            miss_family += 0.07
            refresh_ims_family += 0.02
            client_issue_family += 0.005
            infra_fail_family += 0.003
            rare_family += 0.002
        elif state == "origin_slow":
            hit_family -= 0.06
            miss_family += 0.05
            refresh_ims_family += 0.01
            client_issue_family += 0.004
            infra_fail_family += 0.010
            rare_family += 0.002
        elif state == "network_issue":
            hit_family -= 0.05
            miss_family += 0.02
            refresh_ims_family += 0.005
            client_issue_family += 0.020
            infra_fail_family += 0.018
            rare_family += 0.002
        elif state == "bad_incident":
            hit_family -= 0.16
            miss_family += 0.08
            refresh_ims_family += 0.02
            client_issue_family += 0.020
            infra_fail_family += 0.030
            rare_family += 0.010

        if service in {"live", "live_ott"} and ctype == "segment":
            hit_family = min(hit_family + 0.02, 0.92)
            miss_family = max(miss_family - 0.01, 0.01)
        if service == "app_backend" and ctype == "api":
            infra_fail_family += 0.005
            hit_family -= 0.005

        families = {
            "hit_family": max(0.0, hit_family),
            "miss_family": max(0.0, miss_family),
            "refresh_ims_family": max(0.0, refresh_ims_family),
            "client_issue_family": max(0.0, client_issue_family),
            "infra_fail_family": max(0.0, infra_fail_family),
            "rare_family": max(0.0, rare_family),
        }

        total = sum(families.values())
        return {k: v / total for k, v in families.items()}

    def _ats_code_probs(
        state: str,
        ctype: str,
        service: str,
        cache_hit: float,
    ) -> Dict[str, float]:
        fam = _ats_family_targets(state, ctype, service, cache_hit)

        if ctype == "segment":
            hit_split = {
                "ats_tcp_hit_count": 0.86,
                "ats_tcp_cf_hit_count": 0.14,
            }
            miss_split = {
                "ats_tcp_miss_count": 0.70,
                "ats_tcp_refresh_miss_count": 0.22,
                "ats_tcp_ref_fail_hit_count": 0.08,
            }
            refresh_split = {
                "ats_tcp_refresh_hit_count": 0.36,
                "ats_tcp_client_refresh_count": 0.14,
                "ats_tcp_ims_hit_count": 0.26,
                "ats_tcp_ims_miss_count": 0.24,
            }
        elif ctype == "manifest":
            hit_split = {
                "ats_tcp_hit_count": 0.80,
                "ats_tcp_cf_hit_count": 0.20,
            }
            miss_split = {
                "ats_tcp_miss_count": 0.52,
                "ats_tcp_refresh_miss_count": 0.24,
                "ats_tcp_ref_fail_hit_count": 0.24,
            }
            refresh_split = {
                "ats_tcp_refresh_hit_count": 0.28,
                "ats_tcp_client_refresh_count": 0.22,
                "ats_tcp_ims_hit_count": 0.22,
                "ats_tcp_ims_miss_count": 0.28,
            }
        else:
            hit_split = {
                "ats_tcp_hit_count": 0.72,
                "ats_tcp_cf_hit_count": 0.28,
            }
            miss_split = {
                "ats_tcp_miss_count": 0.76,
                "ats_tcp_refresh_miss_count": 0.14,
                "ats_tcp_ref_fail_hit_count": 0.10,
            }
            refresh_split = {
                "ats_tcp_refresh_hit_count": 0.42,
                "ats_tcp_client_refresh_count": 0.18,
                "ats_tcp_ims_hit_count": 0.20,
                "ats_tcp_ims_miss_count": 0.20,
            }

        if state == "network_issue":
            client_split = {
                "ats_err_client_abort_count": 0.42,
                "ats_err_client_read_error_count": 0.58,
            }
            infra_split = {
                "ats_err_connect_fail_count": 0.24,
                "ats_err_dns_fail_count": 0.10,
                "ats_err_read_timeout_count": 0.66,
            }
        elif state == "origin_slow":
            client_split = {
                "ats_err_client_abort_count": 0.60,
                "ats_err_client_read_error_count": 0.40,
            }
            infra_split = {
                "ats_err_connect_fail_count": 0.26,
                "ats_err_dns_fail_count": 0.08,
                "ats_err_read_timeout_count": 0.66,
            }
        elif state == "bad_incident":
            client_split = {
                "ats_err_client_abort_count": 0.55,
                "ats_err_client_read_error_count": 0.45,
            }
            infra_split = {
                "ats_err_connect_fail_count": 0.30,
                "ats_err_dns_fail_count": 0.12,
                "ats_err_read_timeout_count": 0.58,
            }
        else:
            client_split = {
                "ats_err_client_abort_count": 0.68,
                "ats_err_client_read_error_count": 0.32,
            }
            infra_split = {
                "ats_err_connect_fail_count": 0.34,
                "ats_err_dns_fail_count": 0.10,
                "ats_err_read_timeout_count": 0.56,
            }

        rare_split = {
            "ats_tcp_swapfail_count": 0.20,
            "ats_err_invalid_req_count": 0.28,
            "ats_err_proxy_denied_count": 0.16,
            "ats_err_unknown_count": 0.36,
        }

        probs = {col: 0.0 for col in ATS_COLUMNS}

        for col, frac in hit_split.items():
            probs[col] += fam["hit_family"] * frac
        for col, frac in miss_split.items():
            probs[col] += fam["miss_family"] * frac
        for col, frac in refresh_split.items():
            probs[col] += fam["refresh_ims_family"] * frac
        for col, frac in client_split.items():
            probs[col] += fam["client_issue_family"] * frac
        for col, frac in infra_split.items():
            probs[col] += fam["infra_fail_family"] * frac
        for col, frac in rare_split.items():
            probs[col] += fam["rare_family"] * frac

        total = sum(probs.values())
        return {k: v / total for k, v in probs.items()}

    def _sample_ats_counts(
        requests: int,
        state: str,
        ctype: str,
        service: str,
        cache_hit: float,
    ) -> Dict[str, int]:
        probs = _ats_code_probs(state, ctype, service, cache_hit)
        sampled = rng.multinomial(requests, [probs[col] for col in ATS_COLUMNS])
        return {col: int(sampled[i]) for i, col in enumerate(ATS_COLUMNS)}

    def _ats_family_shares(ats_counts: Dict[str, int], requests: int) -> Dict[str, float]:
        if requests <= 0:
            return {
                "hit_family": 0.0,
                "miss_family": 0.0,
                "refresh_ims": 0.0,
                "client_issues": 0.0,
                "infra": 0.0,
                "rare": 0.0,
            }

        hit_family = (
            ats_counts["ats_tcp_hit_count"]
            + ats_counts["ats_tcp_cf_hit_count"]
        ) / requests

        miss_family = (
            ats_counts["ats_tcp_miss_count"]
            + ats_counts["ats_tcp_refresh_miss_count"]
            + ats_counts["ats_tcp_ref_fail_hit_count"]
        ) / requests

        refresh_ims = (
            ats_counts["ats_tcp_refresh_hit_count"]
            + ats_counts["ats_tcp_client_refresh_count"]
            + ats_counts["ats_tcp_ims_hit_count"]
            + ats_counts["ats_tcp_ims_miss_count"]
        ) / requests

        client_issues = (
            ats_counts["ats_err_client_abort_count"]
            + ats_counts["ats_err_client_read_error_count"]
        ) / requests

        infra = (
            ats_counts["ats_err_connect_fail_count"]
            + ats_counts["ats_err_dns_fail_count"]
            + ats_counts["ats_err_read_timeout_count"]
        ) / requests

        rare = (
            ats_counts["ats_tcp_swapfail_count"]
            + ats_counts["ats_err_invalid_req_count"]
            + ats_counts["ats_err_proxy_denied_count"]
            + ats_counts["ats_err_unknown_count"]
        ) / requests

        return {
            "hit_family": float(hit_family),
            "miss_family": float(miss_family),
            "refresh_ims": float(refresh_ims),
            "client_issues": float(client_issues),
            "infra": float(infra),
            "rare": float(rare),
        }

    # -----------------------------
    # Phase 5 latency shaping
    # -----------------------------
    def _ats_latency_pressure(ats_counts: Dict[str, int], requests: int) -> Dict[str, float]:
        fam = _ats_family_shares(ats_counts, requests)
        return {
            "miss_rate": fam["miss_family"],
            "refresh_ims_rate": fam["refresh_ims"],
            "client_issue_rate": fam["client_issues"],
            "infra_rate": fam["infra"],
            "rare_rate": fam["rare"],
        }

    def _content_latency_scalars(ctype: str) -> Dict[str, float]:
        if ctype == "segment":
            return {
                "base_floor_mult": 1.38,
                "miss_weight": 4.1,
                "refresh_weight": 1.35,
                "client_weight": 2.2,
                "infra_weight": 5.1,
                "rare_weight": 2.3,
                "p50_gain": 0.72,
                "p95_gain": 1.55,
                "p99_gain": 2.30,
            }
        if ctype == "manifest":
            return {
                "base_floor_mult": 1.10,
                "miss_weight": 2.5,
                "refresh_weight": 1.95,
                "client_weight": 1.7,
                "infra_weight": 3.9,
                "rare_weight": 1.8,
                "p50_gain": 0.45,
                "p95_gain": 1.08,
                "p99_gain": 1.68,
            }
        return {
            "base_floor_mult": 0.82,
            "miss_weight": 0.95,
            "refresh_weight": 0.65,
            "client_weight": 0.85,
            "infra_weight": 1.75,
            "rare_weight": 0.95,
            "p50_gain": 0.14,
            "p95_gain": 0.42,
            "p99_gain": 0.76,
        }

    def _apply_latency_pressure(
        p50: float,
        p95: float,
        p99: float,
        ctype: str,
        ats_counts: Dict[str, int],
        requests: int,
        minute_state: str,
    ) -> tuple[float, float, float]:
        pressure = _ats_latency_pressure(ats_counts, requests)
        weights = _content_latency_scalars(ctype)

        severity = (
            pressure["miss_rate"] * weights["miss_weight"]
            + pressure["refresh_ims_rate"] * weights["refresh_weight"]
            + pressure["client_issue_rate"] * weights["client_weight"]
            + pressure["infra_rate"] * weights["infra_weight"]
            + pressure["rare_rate"] * weights["rare_weight"]
        )

        state_boost = {
            "healthy": 1.00,
            "cache_pressure": 1.08,
            "origin_slow": 1.14,
            "network_issue": 1.18,
            "bad_incident": 1.28,
        }[minute_state]

        severity *= state_boost

        p50 *= weights["base_floor_mult"]
        p95 *= weights["base_floor_mult"]
        p99 *= weights["base_floor_mult"]

        if ctype == "segment":
            playback_pressure = pressure["miss_rate"] + pressure["infra_rate"] + (0.45 * pressure["client_issue_rate"])
            p95 *= 1.0 + (playback_pressure * 1.15)
            p99 *= 1.0 + (playback_pressure * 1.55)
        elif ctype == "manifest":
            manifest_pressure = pressure["refresh_ims_rate"] + (0.55 * pressure["miss_rate"]) + (0.35 * pressure["infra_rate"])
            p95 *= 1.0 + (manifest_pressure * 0.42)
            p99 *= 1.0 + (manifest_pressure * 0.58)
        else:
            api_damp = 1.0 - min(0.22, pressure["refresh_ims_rate"] * 0.45)
            p50 *= api_damp
            p95 *= api_damp
            p99 *= api_damp

        p50 *= 1.0 + (severity * weights["p50_gain"])
        p95 *= 1.0 + (severity * weights["p95_gain"])
        p99 *= 1.0 + (severity * weights["p99_gain"])

        return p50, p95, p99

    # -----------------------------
    # Phase 6 correlation helpers
    # -----------------------------
    def _derive_cache_hit_rate(
        base_cache_hit: float,
        ats_counts: Dict[str, int],
        requests: int,
        minute_state: str,
    ) -> float:
        fam = _ats_family_shares(ats_counts, requests)

        ats_cache_view = (
            fam["hit_family"]
            + 0.35 * fam["refresh_ims"]
            - 0.55 * fam["miss_family"]
            - 0.35 * fam["infra"]
            - 0.15 * fam["client_issues"]
        )

        state_shift = {
            "healthy": 0.00,
            "cache_pressure": -0.04,
            "origin_slow": -0.02,
            "network_issue": -0.02,
            "bad_incident": -0.06,
        }[minute_state]

        blended = (
            0.40 * base_cache_hit
            + 0.60 * _clamp(ats_cache_view, 0.02, 0.99)
            + state_shift
        )
        return float(_clamp(blended, 0.02, 0.99))

    def _derive_5xx_rates(
        service: str,
        minute_state: str,
        ats_counts: Dict[str, int],
        requests: int,
    ) -> tuple[float, float, float, float]:
        fam = _ats_family_shares(ats_counts, requests)

        infra = fam["infra"]
        miss = fam["miss_family"]
        rare = fam["rare"]

        rate_500 = 0.0002
        rate_502 = 0.00025
        rate_503 = 0.0002
        rate_504 = 0.0002

        if service == "app_backend":
            rate_500 *= 1.7
            rate_504 *= 1.4

        rate_500 += 0.05 * infra + 0.01 * rare
        rate_502 += 0.09 * infra + 0.012 * rare
        rate_503 += 0.05 * miss + 0.10 * infra + 0.012 * rare
        rate_504 += 0.14 * infra + 0.008 * rare

        state_mult = {
            "healthy": (1.0, 1.0, 1.0, 1.0),
            "cache_pressure": (1.0, 1.1, 1.25, 1.1),
            "origin_slow": (1.15, 1.2, 1.65, 1.35),
            "network_issue": (1.0, 1.25, 1.25, 1.9),
            "bad_incident": (1.6, 1.7, 2.2, 2.0),
        }[minute_state]

        rate_500 *= state_mult[0]
        rate_502 *= state_mult[1]
        rate_503 *= state_mult[2]
        rate_504 *= state_mult[3]

        return (
            float(_clamp(rate_500, 0.0, 0.20)),
            float(_clamp(rate_502, 0.0, 0.20)),
            float(_clamp(rate_503, 0.0, 0.20)),
            float(_clamp(rate_504, 0.0, 0.20)),
        )

    def _derive_crc_errors(
        bytes_sent: int,
        ats_counts: Dict[str, int],
        requests: int,
        minute_state: str,
    ) -> int:
        fam = _ats_family_shares(ats_counts, requests)
        mb = bytes_sent / 1e6

        state_factor = {
            "healthy": 1.00,
            "cache_pressure": 1.08,
            "origin_slow": 1.16,
            "network_issue": 1.55,
            "bad_incident": 1.90,
        }[minute_state]

        lam = (
            mb * 0.0013
            * state_factor
            * (
                1.0
                + 3.2 * fam["infra"]
                + 1.6 * fam["client_issues"]
                + 0.8 * fam["rare"]
            )
        )
        return int(rng.poisson(lam=max(0.0, lam)))

    rows = []

    for m in range(minutes):
        ts = ts0 + timedelta(minutes=m)

        k = max(50, int(len(slice_pool) * density))
        idxs = rng.choice(len(slice_pool), size=k, replace=False)

        for idx in idxs:
            partner, service, region, pop, host, ctype, ua_family = slice_pool[idx]
            minute_state = service_state_timelines[service][m]

            mult = traffic_multiplier(ts, service, ctype) * _state_request_multiplier(minute_state)

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
            pre_ats_cache_hit = float(
                _clamp(rng.normal(base_cache, 0.05) + _state_cache_delta(minute_state), 0.05, 0.99)
            )

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

            lp50, lp95, lp99 = _state_latency_multipliers(minute_state)
            p50 *= lp50
            p95 *= lp95
            p99 *= lp99

            avg_bytes = {"manifest": 18_000, "segment": 900_000, "api": 45_000}.get(ctype, 120_000)
            bytes_sent = int(requests * max(2000.0, rng.normal(avg_bytes, avg_bytes * 0.15)))

            ats_counts = _sample_ats_counts(
                requests=requests,
                state=minute_state,
                ctype=ctype,
                service=service,
                cache_hit=pre_ats_cache_hit,
            )

            cache_hit = _derive_cache_hit_rate(
                base_cache_hit=pre_ats_cache_hit,
                ats_counts=ats_counts,
                requests=requests,
                minute_state=minute_state,
            )

            p50, p95, p99 = _apply_latency_pressure(
                p50=p50,
                p95=p95,
                p99=p99,
                ctype=ctype,
                ats_counts=ats_counts,
                requests=requests,
                minute_state=minute_state,
            )

            rate_500, rate_502, rate_503, rate_504 = _derive_5xx_rates(
                service=service,
                minute_state=minute_state,
                ats_counts=ats_counts,
                requests=requests,
            )

            status_500 = int(rng.binomial(requests, rate_500))
            status_502 = int(rng.binomial(requests, rate_502))
            status_503 = int(rng.binomial(requests, rate_503))
            status_504 = int(rng.binomial(requests, rate_504))

            http_5xx_tmp = status_500 + status_502 + status_503 + status_504
            max_5xx_allowed = int(requests * 0.40)
            if http_5xx_tmp > max_5xx_allowed and http_5xx_tmp > 0:
                scale = max_5xx_allowed / http_5xx_tmp
                status_500 = int(round(status_500 * scale))
                status_502 = int(round(status_502 * scale))
                status_503 = int(round(status_503 * scale))
                status_504 = int(round(status_504 * scale))

            crc_errors = _derive_crc_errors(
                bytes_sent=bytes_sent,
                ats_counts=ats_counts,
                requests=requests,
                minute_state=minute_state,
            )

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
                    crc_errors += int(max(0.0, (bytes_sent / 1e6) * (0.25 * inten)))

            p95 = max(p95, p50)
            p99 = max(p99, p95)

            http_5xx = status_500 + status_502 + status_503 + status_504
            remaining = max(0, requests - http_5xx)

            base_4xx = 0.004
            if service == "app_backend":
                base_4xx *= 2.0
            if ctype == "api":
                base_4xx *= 1.5

            if minute_state == "network_issue":
                base_4xx *= 1.20
            elif minute_state == "bad_incident":
                base_4xx *= 1.35

            http_4xx = int(rng.binomial(remaining, min(base_4xx, 0.25)))
            remaining -= http_4xx

            base_3xx = 0.02
            if ctype == "manifest":
                base_3xx *= 1.3
            if ctype == "api":
                base_3xx *= 1.1

            http_3xx = int(rng.binomial(remaining, min(base_3xx, 0.40)))
            remaining -= http_3xx

            http_2xx = max(0, remaining)

            if (http_2xx + http_3xx + http_4xx + http_5xx) != requests:
                http_2xx = max(0, requests - (http_3xx + http_4xx + http_5xx))

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

            status_304 = http_3xx

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

            if (status_200 + status_206) != http_2xx:
                status_200 = max(0, http_2xx - status_206)
            if status_304 != http_3xx:
                status_304 = http_3xx
            if (status_403 + status_404 + status_429) != http_4xx:
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
                    "requests": int(requests),
                    "bytes_sent": int(bytes_sent),
                    "p50_ms": float(p50),
                    "p95_ms": float(p95),
                    "p99_ms": float(p99),
                    "cache_hit_rate": float(cache_hit),
                    "http_2xx_count": int(http_2xx),
                    "http_3xx_count": int(http_3xx),
                    "http_4xx_count": int(http_4xx),
                    "http_5xx_count": int(http_5xx),
                    "status_200": int(status_200),
                    "status_206": int(status_206),
                    "status_304": int(status_304),
                    "status_403": int(status_403),
                    "status_404": int(status_404),
                    "status_429": int(status_429),
                    "status_500": int(status_500),
                    "status_502": int(status_502),
                    "status_503": int(status_503),
                    "status_504": int(status_504),
                    "crc_errors": int(crc_errors),
                    **ats_counts,
                }
            )

    df = pd.DataFrame(rows)
    if not df.empty:
        df["ts"] = pd.to_datetime(df["ts"], utc=True)
    return df
