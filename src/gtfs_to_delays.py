"""Generate synthetic delay events using real GTFS schedules.

This module reads a GTFS static feed (ZIP with routes.txt, trips.txt, stop_times.txt)
and creates a realistic-looking `data/raw/delays.csv` that matches the
schema expected by `src.data_ingestion`:

    timestamp,route_id,trip_id,stop_id,scheduled_departure,actual_departure,mode

The schedules (routes, trips, stops, departure times) are real, taken from
GTFS. The delays (difference between scheduled and actual departure) are
synthetic but sampled from a plausible distribution.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
import zipfile

import numpy as np
import pandas as pd


DEFAULT_GTFS_ZIP = Path("data/raw/gtt_gtfs.zip")
DEFAULT_OUTPUT = Path("data/raw/delays.csv")
DEFAULT_SERVICE_DATE = "2025-09-01"  # arbitrary; times are realistic within a day


@dataclass
class GTFSConfig:
    gtfs_zip: Path = DEFAULT_GTFS_ZIP
    output_csv: Path = DEFAULT_OUTPUT
    service_date: str = DEFAULT_SERVICE_DATE
    max_rows: int = 20000


def _safe_time_to_timedelta(time_str: str) -> pd.Timedelta:
    """Convert a GTFS HH:MM:SS (possibly >= 24h) into a timedelta.

    GTFS allows hours >= 24 to represent trips after midnight. We fold
    hours modulo 24 here; for our synthetic example this is sufficient.
    """

    if not isinstance(time_str, str):
        return pd.NaT  # type: ignore[return-value]

    parts = time_str.split(":")
    if len(parts) != 3:
        return pd.NaT  # type: ignore[return-value]
    h, m, s = parts
    try:
        h_i = int(h) % 24
        m_i = int(m)
        s_i = int(s)
    except ValueError:
        return pd.NaT  # type: ignore[return-value]

    return pd.Timedelta(hours=h_i, minutes=m_i, seconds=s_i)


def _load_gtfs_tables(gtfs_zip: Path) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if not gtfs_zip.exists():
        raise FileNotFoundError(
            f"GTFS zip not found at {gtfs_zip}. Download it first (e.g. to data/raw/gtt_gtfs.zip)."
        )

    with zipfile.ZipFile(gtfs_zip) as zf:
        with zf.open("stop_times.txt") as f:
            stop_times = pd.read_csv(f)
        with zf.open("trips.txt") as f:
            trips = pd.read_csv(f)
        with zf.open("routes.txt") as f:
            routes = pd.read_csv(f)

    return stop_times, trips, routes


def _route_type_to_mode(route_type: int) -> str:
    """Map GTFS route_type to a human-readable mode label."""

    mapping = {
        0: "tram",  # Tram, Streetcar, Light rail
        1: "metro",  # Subway, Metro
        2: "rail",  # Rail
        3: "bus",
        4: "ferry",
        5: "cable_car",
        6: "gondola",
        7: "funicular",
    }
    return mapping.get(int(route_type), "bus")


def build_synthetic_delays(config: GTFSConfig) -> pd.DataFrame:
    """Build a delays DataFrame from real GTFS schedules with synthetic delays."""

    stop_times, trips, routes = _load_gtfs_tables(config.gtfs_zip)

    # Join stop_times -> trips -> routes to get route_id and route_type
    df = stop_times.merge(trips[["trip_id", "route_id"]], on="trip_id", how="left")
    df = df.merge(routes[["route_id", "route_type"]], on="route_id", how="left")

    # Compute scheduled departure timestamps for a single service date
    service_date = pd.to_datetime(config.service_date)
    td = df["departure_time"].apply(_safe_time_to_timedelta)
    df = df[~td.isna()].copy()
    df["scheduled_departure"] = service_date + td[~td.isna()].values

    # Map route_type to a simple mode string
    df["mode"] = df["route_type"].fillna(3).astype(int).apply(_route_type_to_mode)

    # Optionally subsample to keep the dataset manageable
    if config.max_rows and len(df) > config.max_rows:
        df = df.sample(config.max_rows, random_state=42).reset_index(drop=True)

    # Generate synthetic delays (minutes): mostly small, some large positive "bad" delays
    n = len(df)
    rng = np.random.default_rng(42)

    base = rng.normal(loc=1.5, scale=3.0, size=n)  # around 1.5 min late with some spread
    heavy_tail = rng.exponential(scale=4.0, size=n)  # occasional big lates
    bad_mask = rng.random(n) < 0.15  # 15% of events are significantly delayed

    delays_min = base
    delays_min[bad_mask] += heavy_tail[bad_mask]
    # Clamp to a reasonable range: up to 25 min late, down to 3 min early
    delays_min = np.clip(delays_min, -3.0, 25.0)

    delay_td = pd.to_timedelta(delays_min, unit="m")
    df["actual_departure"] = df["scheduled_departure"] + delay_td

    # Use scheduled_departure as observation timestamp for simplicity
    df["timestamp"] = df["scheduled_departure"]

    # Build the final schema expected by data_ingestion
    out = pd.DataFrame(
        {
            "timestamp": df["timestamp"],
            "route_id": df["route_id"],
            "trip_id": df["trip_id"],
            "stop_id": df["stop_id"],
            "scheduled_departure": df["scheduled_departure"],
            "actual_departure": df["actual_departure"],
            "mode": df["mode"],
        }
    )

    out = out.sort_values(["timestamp", "route_id", "trip_id"]).reset_index(drop=True)
    return out


def run(config: Optional[GTFSConfig] = None) -> Path:
    if config is None:
        config = GTFSConfig()

    df = build_synthetic_delays(config)
    config.output_csv.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(config.output_csv, index=False)
    return config.output_csv


def _parse_args(argv: Optional[list[str]] = None) -> GTFSConfig:
    parser = argparse.ArgumentParser(
        description=(
            "Generate synthetic delay events from a GTFS static feed "
            "into data/raw/delays.csv"
        )
    )
    parser.add_argument(
        "--gtfs-zip",
        type=str,
        default=str(DEFAULT_GTFS_ZIP),
        help="Path to GTFS static ZIP (default: data/raw/gtt_gtfs.zip)",
    )
    parser.add_argument(
        "--output-csv",
        type=str,
        default=str(DEFAULT_OUTPUT),
        help="Output CSV path for synthetic delays (default: data/raw/delays.csv)",
    )
    parser.add_argument(
        "--service-date",
        type=str,
        default=DEFAULT_SERVICE_DATE,
        help="Service date (YYYY-MM-DD) used to anchor departure times.",
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        default=20000,
        help="Maximum number of delay events to sample from GTFS (0 = no limit)",
    )

    args = parser.parse_args(argv)
    return GTFSConfig(
        gtfs_zip=Path(args.gtfs_zip),
        output_csv=Path(args.output_csv),
        service_date=args.service_date,
        max_rows=args.max_rows,
    )


def main(argv: Optional[list[str]] = None) -> None:
    config = _parse_args(argv)
    out_path = run(config)
    print(f"Wrote synthetic delays CSV to {out_path}")


if __name__ == "__main__":  # pragma: no cover
    main()
