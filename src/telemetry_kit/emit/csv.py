from __future__ import annotations

from pathlib import Path
from typing import Iterable, Optional

import pandas as pd

from ..schema import RAW_MINUTE_COLUMNS


def write_raw_minute_csv(
    df: pd.DataFrame,
    out_path: str | Path,
    columns: Optional[Iterable[str]] = None,
) -> Path:
    """
    Write raw_minute telemetry to CSV with stable column order.
    - Ensures columns exist (adds missing as null)
    - Formats ts as ISO UTC string
    """
    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)

    cols = list(columns) if columns is not None else RAW_MINUTE_COLUMNS

    # Add any missing columns as nulls
    for c in cols:
        if c not in df.columns:
            df[c] = None

    # Stable ordering
    df_out = df[list(cols)].copy()

    # Format timestamp
    if "ts" in df_out.columns:
        df_out["ts"] = pd.to_datetime(df_out["ts"], utc=True).dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    df_out.to_csv(out, index=False)
    return out
