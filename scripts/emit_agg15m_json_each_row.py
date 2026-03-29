    #!/usr/bin/env python3
    from __future__ import annotations

    import os, sys
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

    import argparse
    import json
    from datetime import datetime, timedelta, timezone

    import pandas as pd

    from telemetry_kit.generator import generate_minute_logs, aggregate_logs
    from telemetry_kit.schema import AGG_15M_COLUMNS


    def to_ch_utc_str(dt: datetime) -> str:
        """ClickHouse-friendly UTC: 'YYYY-MM-DD HH:MM:SS' (no tz suffix)."""
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt.strftime("%Y-%m-%d %H:%M:%S")


    def parse_start(s: str) -> datetime:
        s = s.replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)


    def main():
        ap = argparse.ArgumentParser()
        ap.add_argument("--minutes", type=int, default=24 * 60)  # total minutes to cover
        ap.add_argument("--seed", type=int, default=7)
        ap.add_argument("--start", type=str, default="2026-02-20T00:00:00Z")
        ap.add_argument("--density", type=float, default=0.10)
        ap.add_argument("--bucket", type=int, default=15)
        args = ap.parse_args()

        start_ts = parse_start(args.start)

        bucket = int(args.bucket)
        if bucket <= 0 or (args.minutes % bucket) != 0:
            # Keep it strict so we don't emit partial buckets accidentally.
            raise SystemExit(f"--minutes must be a multiple of --bucket (bucket={bucket})")

        # Stream in bucket-sized chunks (VPS-friendly)
        try:
            for offset in range(0, args.minutes, bucket):
                chunk_start = start_ts + timedelta(minutes=offset)

                raw = generate_minute_logs(
                    start_ts_utc=chunk_start,
                    minutes=bucket,
                    seed=args.seed,
                    density=args.density,
                )
                if raw.empty:
                    continue

                agg = aggregate_logs(raw, bucket_minutes=bucket)
                if agg.empty:
                    continue

                # enforce contract order + fix time
                agg = agg[AGG_15M_COLUMNS].copy()
                agg["ts"] = pd.to_datetime(agg["ts"], utc=True).dt.to_pydatetime()
                agg["ts"] = agg["ts"].apply(to_ch_utc_str)

                for row in agg.to_dict(orient="records"):
                    print(json.dumps(row, separators=(",", ":"), ensure_ascii=False))
        except BrokenPipeError:
            # When piping to `head`, stdout closes early — exit quietly.
            sys.exit(0)


    if __name__ == "__main__":
        main()