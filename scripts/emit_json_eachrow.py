#!/usr/bin/env python3
from __future__ import annotations

import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))


import argparse
import json
from datetime import datetime, timezone

from telemetry_kit.generator import generate_minute_logs
from telemetry_kit.schema import RAW_MINUTE_COLUMNS

def to_ch_utc_str(dt: datetime) -> str:
    """ClickHouse-friendly UTC: 'YYYY-MM-DD HH:MM:SS' (no tz suffix)."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def parse_start(s: str) -> datetime:
    # Accept "2026-02-20T00:00:00Z" or with offset
    s = s.replace("Z", "+00:00")
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--minutes", type=int, default=60)
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument(
        "--start",
        type=str,
        default="2026-02-20T00:00:00Z",
        help="Fixed UTC start for deterministic replay (ISO, e.g. 2026-02-20T00:00:00Z)",
    )
    ap.add_argument("--density", type=float, default=0.10)
    args = ap.parse_args()

    start_ts = parse_start(args.start)

    df = generate_minute_logs(
        start_ts_utc=start_ts,
        minutes=args.minutes,
        seed=args.seed,
        density=args.density,
    )

    if df.empty:
        return

    # enforce contract order + fix time
    df = df[RAW_MINUTE_COLUMNS].copy()
    df["ts"] = df["ts"].apply(to_ch_utc_str)

    # JSONEachRow: one JSON per line
    for row in df.to_dict(orient="records"):
        print(json.dumps(row, separators=(",", ":"), ensure_ascii=False))


if __name__ == "__main__":
    main()