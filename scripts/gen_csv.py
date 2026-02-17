from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone

from telemetry_kit.generator import generate_minute_logs
from telemetry_kit.emit.csv import write_raw_minute_csv


def main() -> None:
    ap = argparse.ArgumentParser(description="Generate synthetic CDN telemetry CSV (raw_minute).")
    ap.add_argument("--out", required=True, help="Output CSV path (e.g. /tmp/telemetry.csv)")
    ap.add_argument("--minutes", type=int, default=360, help="Minutes to generate")
    ap.add_argument("--seed", type=int, default=7, help="Random seed for reproducibility")
    ap.add_argument("--density", type=float, default=0.10, help="Slice sampling density (0-1)")
    ap.add_argument("--partners", type=int, default=6)
    ap.add_argument("--pops", type=int, default=20)
    ap.add_argument("--hosts", type=int, default=120)
    args = ap.parse_args()

    now = datetime.now(timezone.utc)
    start = (now - timedelta(minutes=args.minutes)).replace(second=0, microsecond=0)

    df = generate_minute_logs(
        start_ts_utc=start,
        minutes=args.minutes,
        n_partners=args.partners,
        n_pops=args.pops,
        n_hosts=args.hosts,
        seed=args.seed,
        density=args.density,
        incidents=[],  # keep empty by default
    )

    out = write_raw_minute_csv(df, args.out)
    print(f"Wrote {len(df):,} rows -> {out}")


if __name__ == "__main__":
    main()
