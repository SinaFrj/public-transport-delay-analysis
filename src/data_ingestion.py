"""Data ingestion utilities for public transport delay analysis.

This module expects a raw CSV file with at least the following columns:
- timestamp: ISO datetime when the event is observed.
- route_id: identifier of the route/line.
- trip_id: trip or vehicle run identifier.
- stop_id: stop identifier (optional).
- scheduled_departure: scheduled departure datetime.
- actual_departure: actual departure datetime.
- mode: transport mode (e.g., "bus", "tram").

The CLI entrypoint reads a raw CSV from data/raw/delays.csv by default,
cleans it, computes delay_minutes, and writes a processed Parquet file
under data/processed/delays.parquet.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pandas as pd


RAW_DEFAULT = Path("data/raw/delays.csv")
PROCESSED_DEFAULT = Path("data/processed/delays.parquet")


@dataclass
class IngestionConfig:
    input_path: Path = RAW_DEFAULT
    output_path: Path = PROCESSED_DEFAULT


def load_raw_delays(path: Path) -> pd.DataFrame:
    """Load raw delay events from CSV.

    Datetime-like columns are parsed; unknown columns are preserved.
    """

    if not path.exists():
        raise FileNotFoundError(
            f"Raw delays file not found: {path}. "
            "Provide a CSV with delay events (see data_ingestion module docstring)."
        )

    df = pd.read_csv(
        path,
        parse_dates=[
            "timestamp",
            "scheduled_departure",
            "actual_departure",
        ],
        infer_datetime_format=True,
    )
    return df


def _compute_delay_minutes(df: pd.DataFrame) -> pd.Series:
    """Compute delay in minutes as (actual - scheduled).

    Negative values (early departures) are kept; downstream code may clamp if desired.
    """

    if "actual_departure" not in df or "scheduled_departure" not in df:
        raise KeyError("Both 'actual_departure' and 'scheduled_departure' must be present")

    delta = df["actual_departure"] - df["scheduled_departure"]
    return delta.dt.total_seconds() / 60.0


def clean_delays(df: pd.DataFrame) -> pd.DataFrame:
    """Clean raw delay events and compute helper fields.

    - Ensures required columns are present.
    - Drops rows with missing departure times.
    - Computes delay_minutes.
    - Adds date and hour-of-day helper columns for aggregation.
    """

    required = {
        "timestamp",
        "route_id",
        "trip_id",
        "scheduled_departure",
        "actual_departure",
    }
    missing = required - set(df.columns)
    if missing:
        raise KeyError(f"Missing required columns in raw delays: {sorted(missing)}")

    df = df.copy()

    # Drop rows without times
    df = df.dropna(subset=["scheduled_departure", "actual_departure"])

    df["delay_minutes"] = _compute_delay_minutes(df)

    # Helper columns for grouping
    df["date"] = df["timestamp"].dt.date
    df["hour"] = df["timestamp"].dt.hour

    return df


def save_processed(df: pd.DataFrame, path: Path) -> None:
    """Save cleaned delays to Parquet (preferred) or CSV as a fallback."""

    path.parent.mkdir(parents=True, exist_ok=True)

    if path.suffix == ".parquet":
        try:
            df.to_parquet(path, index=False)
        except Exception:
            # Fallback to CSV if Parquet is not available in the environment.
            csv_path = path.with_suffix(".csv")
            df.to_csv(csv_path, index=False)
    else:
        df.to_csv(path, index=False)


def run_pipeline(config: Optional[IngestionConfig] = None) -> Path:
    """Run the ingestion pipeline end-to-end and return the output path."""

    if config is None:
        config = IngestionConfig()

    raw_df = load_raw_delays(config.input_path)
    cleaned = clean_delays(raw_df)
    save_processed(cleaned, config.output_path)
    return config.output_path


def _parse_args(argv: Optional[list[str]] = None) -> IngestionConfig:
    parser = argparse.ArgumentParser(description="Ingest and clean delay events data.")
    parser.add_argument(
        "--input",
        type=str,
        default=str(RAW_DEFAULT),
        help="Path to raw delays CSV (default: data/raw/delays.csv)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=str(PROCESSED_DEFAULT),
        help="Path for processed dataset (default: data/processed/delays.parquet)",
    )

    args = parser.parse_args(argv)
    return IngestionConfig(input_path=Path(args.input), output_path=Path(args.output))


def main(argv: Optional[list[str]] = None) -> None:
    config = _parse_args(argv)
    out = run_pipeline(config)
    print(f"Wrote processed delays dataset to {out}")


if __name__ == "__main__":  # pragma: no cover
    main()