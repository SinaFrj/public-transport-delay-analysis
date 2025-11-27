"""Streamlit dashboard for public transport delay KPIs.

Run with:
    streamlit run dashboard/app.py

This app expects that the ingestion pipeline has been run first:
    python -m src.data_ingestion
"""

from __future__ import annotations

from pathlib import Path
import sys

import pandas as pd
import streamlit as st
import altair as alt

# Ensure project root (containing the `src` package) is on sys.path when
# Streamlit runs this file directly.
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src import kpi_aggregation


PROCESSED_DEFAULT = kpi_aggregation.PROCESSED_DEFAULT


@st.cache_data(show_spinner=False)
def load_data(path: Path = PROCESSED_DEFAULT) -> pd.DataFrame:
    return kpi_aggregation.load_processed_delays(path)


def main() -> None:
    st.set_page_config(
        page_title="Public Transport Delay Analysis",
        page_icon="🚌",
        layout="wide",
    )

    # Hero header
    st.markdown(
        """
        <h1 style="margin-bottom: 0.2rem;">Public Transport Delay Analysis</h1>
        <p style="color: #666; margin-top: 0;">
        Real GTFS schedules + synthetic delays &nbsp;·&nbsp; Explore reliability by route, mode, and time of day
        </p>
        """,
        unsafe_allow_html=True,
    )

    # Load data
    try:
        df = load_data()
    except FileNotFoundError as e:
        st.error(
            "Processed delays dataset not found. "
            "Run `python -m src.data_ingestion` first to generate data/processed/delays.parquet."
        )
        st.exception(e)
        return

    # Optionally drop funicular from the dataset entirely
    if "mode" in df.columns:
        df = df[df["mode"] != "funicular"].copy()

    # Sidebar filters
    st.sidebar.header("Filters")

    routes = sorted(df["route_id"].dropna().unique().tolist())
    selected_routes = st.sidebar.multiselect("Routes", routes, default=routes)

    modes = df["mode"].dropna().unique().tolist() if "mode" in df.columns else []
    selected_modes = (
        st.sidebar.multiselect("Modes", modes, default=modes) if modes else None
    )

    if "date" in df.columns:
        min_date = df["date"].min()
        max_date = df["date"].max()
        date_range = st.sidebar.date_input(
            "Date range",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date,
        )
    else:
        date_range = None

    # Quick dataset summary in sidebar
    st.sidebar.markdown("---")
    st.sidebar.markdown(
        f"**Total events:** {len(df):,}<br>"
        f"**Routes:** {df['route_id'].nunique():,}",
        unsafe_allow_html=True,
    )

    # Apply filters
    mask = df["route_id"].isin(selected_routes)

    if selected_modes is not None and selected_modes:
        mask &= df["mode"].isin(selected_modes)

    if date_range is not None and isinstance(date_range, tuple) and len(date_range) == 2:
        start_date, end_date = date_range
        mask &= (df["date"] >= start_date) & (df["date"] <= end_date)

    filtered = df[mask]

    if filtered.empty:
        st.warning("No data for the selected filters.")
        return

    # Pre-compute KPIs
    route_kpis = kpi_aggregation.compute_route_kpis(filtered)
    # Sort by reliability score (lowest first = worst reliability)
    route_kpis_display = route_kpis.sort_values("reliability_score", ascending=True)

    avg_delay = route_kpis["avg_delay_min"].mean()
    p90_delay = route_kpis["p90_delay_min"].mean()
    on_time_rate = route_kpis["on_time_rate"].mean()
    avg_reliability = route_kpis["reliability_score"].mean()

    # Time-of-day (peak vs off-peak) KPIs
    period_kpis = kpi_aggregation.compute_route_period_kpis(filtered)

    # Layout: tabs for overview, routes, time-of-day profile, and peak analysis
    tab_overview, tab_routes, tab_time, tab_peak = st.tabs([
        "Overview",
        "Routes table",
        "Time of day profile",
        "Peak vs off-peak",
    ])

    with tab_overview:
        st.subheader("KPIs for current selection")

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Average delay (min)", f"{avg_delay:0.2f}")
        c2.metric("P90 delay (min)", f"{p90_delay:0.2f}")
        c3.metric("On-time rate", f"{on_time_rate * 100:0.1f}%")
        c4.metric("Reliability score", f"{avg_reliability:0.1f}/100")

        # Distribution of delays
        st.markdown("### Delay distribution")
        delay_hist = alt.Chart(filtered).mark_bar(color="#FF4B4B").encode(
            x=alt.X("delay_minutes:Q", bin=alt.Bin(maxbins=40), title="Delay (minutes)"),
            y=alt.Y("count():Q", title="Number of events"),
            tooltip=["count():Q"],
        ).properties(height=260)
        st.altair_chart(delay_hist, use_container_width=True)

        # Top N least reliable routes
        st.markdown("### Least reliable routes")
        top_n = 15
        worst = route_kpis_display.head(top_n)
        if not worst.empty:
            worst_chart = (
                alt.Chart(worst)
                .mark_bar()
                .encode(
                    x=alt.X("reliability_score:Q", title="Reliability score"),
                    y=alt.Y("route_id:N", sort="x", title="Route"),
                    color=alt.value("#1f77b4"),
                    tooltip=[
                        "route_id:N",
                        "reliability_score:Q",
                        "avg_delay_min:Q",
                        "on_time_rate:Q",
                        "n_events:Q",
                    ],
                )
                .properties(height=300)
            )
            st.altair_chart(worst_chart, use_container_width=True)

    with tab_routes:
        st.subheader("Per-route KPIs")
        st.dataframe(
            route_kpis_display,
            use_container_width=True,
        )

    with tab_time:
        st.subheader("Delay by hour of day")

        route_for_hour = st.selectbox(
            "Route for hourly profile",
            options=route_kpis_display["route_id"].tolist(),
        )

        hourly_df = filtered[filtered["route_id"] == route_for_hour]
        hour_kpis = kpi_aggregation.compute_route_hour_kpis(hourly_df)
        hour_kpis = hour_kpis.sort_values("hour")

        if hour_kpis.empty:
            st.info("No data for this route at the selected filters.")
        else:
            line = (
                alt.Chart(hour_kpis)
                .mark_line(point=True)
                .encode(
                    x=alt.X("hour:O", title="Hour of day"),
                    y=alt.Y("avg_delay_min:Q", title="Average delay (min)"),
                    tooltip=[
                        "hour:O",
                        "avg_delay_min:Q",
                        "p90_delay_min:Q",
                        "on_time_rate:Q",
                        "reliability_score:Q",
                    ],
                    color=alt.value("#FF4B4B"),
                )
                .properties(height=320)
            )
            st.altair_chart(line, use_container_width=True)

    with tab_peak:
        st.subheader("Peak vs off-peak reliability")

        route_for_peak = st.selectbox(
            "Route for peak analysis",
            options=route_kpis_display["route_id"].tolist(),
        )

        route_period = period_kpis[period_kpis["route_id"] == route_for_peak]

        if route_period.empty:
            st.info("No data for this route at the selected filters.")
        else:
            # Order buckets in a logical sequence
            cat_order = [
                "Morning peak",
                "Midday",
                "Evening peak",
                "Night / off-peak",
            ]

            bar = (
                alt.Chart(route_period)
                .mark_bar()
                .encode(
                    x=alt.X(
                        "time_of_day:N",
                        title="Time of day",
                        sort=cat_order,
                    ),
                    y=alt.Y(
                        "reliability_score:Q",
                        title="Reliability score",
                    ),
                    color=alt.value("#1f77b4"),
                    tooltip=[
                        "time_of_day:N",
                        "reliability_score:Q",
                        "avg_delay_min:Q",
                        "p90_delay_min:Q",
                        "on_time_rate:Q",
                        "n_events:Q",
                    ],
                )
                .properties(height=320)
            )
            st.altair_chart(bar, use_container_width=True)


if __name__ == "__main__":  # pragma: no cover
    main()