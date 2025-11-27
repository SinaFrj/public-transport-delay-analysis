"""KPI aggregation for public transport delay analysis.

This module operates on the processed dataset produced by `src.data_ingestion`.
By default it expects `data/processed/delays.parquet` (or CSV fallback).

The main KPIs per group (route, hour) are:
- avg_delay_min: mean delay in minutes.
- p90_delay_min: 90th percentile of delay in minutes.
- on_time_rate: share of events with delay_minutes <= threshold.
- n_events: number of events in the group.
- reliability_score: composite 0–100 score combining delay and on-time performance.

Additional helpers compute KPIs by hour-of-day and time-of-day buckets (e.g.,
"Morning peak", "Midday", "Evening peak", "Night / off-peak").
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pandas as pd

from . import data_ingestion


PROCESSED_DEFAULT = data_ingestion.PROCESSED_DEFAULT


@dataclass
class AggregationConfig:
    input_path: Path = PROCESSED_DEFAULT
    output_route_kpis: Optional[Path] = Path("data/processed/route_kpis.csv")


def load_processed_delays(path: Path = PROCESSED_DEFAULT) -> pd.DataFrame:
    """Load processed delays from Parquet or CSV."""

    if not path.exists():
        # Try CSV with same stem as Parquet fallback.
        csv_fallback = path.with_suffix(".csv") if path.suffix == ".parquet" else path
        if not csv_fallback.exists():
            raise FileNotFoundError(
                f"Processed delays dataset not found. Expected {path} or {csv_fallback}. "
                "Run `python -m src.data_ingestion` first."
            )
        path = csv_fallback

    if path.suffix == ".parquet":
        return pd.read_parquet(path)

    return pd.read_csv(path, parse_dates=["timestamp", "scheduled_departure", "actual_departure"])


def _group_kpis(group: pd.DataFrame, on_time_threshold_min: float) -> pd.Series:
    delay = group["delay_minutes"].dropna()
    if delay.empty:
        return pd.Series(
            {
                "avg_delay_min": float("nan"),
                "p90_delay_min": float("nan"),
                "on_time_rate": float("nan"),
                "n_events": 0,
            }
        )

    avg = delay.mean()
    p90 = delay.quantile(0.9)
    on_time = (delay <= on_time_threshold_min).mean()

    return pd.Series(
        {
            "avg_delay_min": avg,
            "p90_delay_min": p90,
            "on_time_rate": on_time,
            "n_events": int(delay.shape[0]),
        }
    )


def _add_reliability_score(kpis: pd.DataFrame) -> pd.DataFrame:
    """Add a composite 0–100 reliability_score to a KPI dataframe.

    Heuristic formula (per row):
        - delay_penalty = clip(avg_delay_min, 0, 10) / 10
        - score = 100 * (0.6 * on_time_rate + 0.4 * (1 - delay_penalty))

    This keeps the score between 0 and 100, with higher better.
    """

    kpis = kpis.copy()
    delay_penalty = (
        kpis["avg_delay_min"].clip(lower=0, upper=10) / 10.0
    )  # 0 (no delay) to 1 (>=10 min)
    on_time = kpis["on_time_rate"].fillna(0)

    score = 100.0 * (0.6 * on_time + 0.4 * (1.0 - delay_penalty.fillna(1.0)))
    kpis["reliability_score"] = score
    return kpis


def _add_time_of_day_bucket(df: pd.DataFrame) -> pd.DataFrame:
    """Add a categorical time_of_day bucket column based on `hour`.

    Buckets (inclusive of start, exclusive of end):
        - Morning peak: 07–10
        - Midday:       10–16
        - Evening peak: 16–19
        - Night / off-peak: everything else
    """

    if "hour" not in df:
        raise KeyError("Dataframe must contain 'hour' (see src.data_ingestion.clean_delays)")

    def _bucket(h: float) -> str:
        if pd.isna(h):
            return "Unknown"
        h_i = int(h)
        if 7 <= h_i < 10:
            return "Morning peak"
        if 10 <= h_i < 16:
            return "Midday"
        if 16 <= h_i < 19:
            return "Evening peak"
        return "Night / off-peak"

    df = df.copy()
    df["time_of_day"] = df["hour"].apply(_bucket)
    return df


def compute_route_kpis(df: pd.DataFrame, on_time_threshold_min: float = 1.0) -> pd.DataFrame:
    """Compute KPIs at route level."""

    if "route_id" not in df or "delay_minutes" not in df:
        raise KeyError("Dataframe must contain 'route_id' and 'delay_minutes'")

    grouped = df.groupby("route_id", dropna=False)
    kpis = grouped.apply(_group_kpis, on_time_threshold_min=on_time_threshold_min)
    kpis = kpis.reset_index()
    kpis = _add_reliability_score(kpis)
    return kpis


def compute_route_hour_kpis(df: pd.DataFrame, on_time_threshold_min: float = 1.0) -> pd.DataFrame:
    """Compute KPIs at (route, hour) level for diurnal patterns."""

    if "hour" not in df:
        raise KeyError("Dataframe must contain 'hour' (see src.data_ingestion.clean_delays)")

    grouped = df.groupby(["route_id", "hour"], dropna=False)
    kpis = grouped.apply(_group_kpis, on_time_threshold_min=on_time_threshold_min)
    kpis = kpis.reset_index()
    kpis = _add_reliability_score(kpis)
    return kpis


def compute_route_period_kpis(df: pd.DataFrame, on_time_threshold_min: float = 1.0) -> pd.DataFrame:
    """Compute KPIs at (route, time_of_day) level (peak vs off-peak)."""

    df_bucketed = _add_time_of_day_bucket(df)
    grouped = df_bucketed.groupby(["route_id", "time_of_day"], dropna=False)
    kpis = grouped.apply(_group_kpis, on_time_threshold_min=on_time_threshold_min)
    kpis = kpis.reset_index()
    kpis = _add_reliability_score(kpis)
    return kpis


def summarize_worst_routes(route_kpis: pd.DataFrame, top_n: int = 10) -> pd.DataFrame:
    """Return the worst-performing routes by average delay."""

    return route_kpis.sort_values("avg_delay_min", ascending=False).head(top_n)


def run_aggregation(config: Optional[AggregationConfig] = None) -> pd.DataFrame:
    """Load processed delays, compute route KPIs, and optionally write CSV."""

    if config is None:
        config = AggregationConfig()

    df = load_processed_delays(config.input_path)
    route_kpis = compute_route_kpis(df)

    if config.output_route_kpis is not None:
        config.output_route_kpis.parent.mkdir(parents=True, exist_ok=True)
        route_kpis.to_csv(config.output_route_kpis, index=False)

    return route_kpis


def _parse_args(argv: Optional[list[str]] = None) -> AggregationConfig:
    parser = argparse.ArgumentParser(description="Compute delay KPIs from processed dataset.")
    parser.add_argument(
        "--input",
        type=str,
        default=str(PROCESSED_DEFAULT),
        help="Path to processed delays dataset (default: data/processed/delays.parquet)",
    )
    parser.add_argument(
        "--output-route-kpis",
        type=str,
        default="data/processed/route_kpis.csv",
        help="Path to write per-route KPI summary CSV (default: data/processed/route_kpis.csv)",
    )
    parser.add_argument(
        "--no-write",
        action="store_true",
        help="Do not write any CSV output, only print summary to stdout.",
    )

    args = parser.parse_args(argv)
    output = None if args.no_write else Path(args.output_route_kpis)
    return AggregationConfig(input_path=Path(args.input), output_route_kpis=output)


def main(argv: Optional[list[str]] = None) -> None:
    config = _parse_args(argv)
    route_kpis = run_aggregation(config)

    worst = summarize_worst_routes(route_kpis, top_n=10)
    print("Top routes by average delay (minutes):")
    print(worst.to_string(index=False, float_format=lambda x: f"{x:0.2f}"))


if __name__ == "__main__":  # pragma: no cover
    main()