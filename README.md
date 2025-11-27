# Public Transport Delay Analysis

Data Science & ML project analyzing live bus & tram delays for Torino’s GTT network (Politecnico di Torino group project).

## Project Overview
- Build a data pipeline to collect and analyze live bus & tram delays from Torino’s GTT GTFS feeds.
- Develop Python scripts for data ingestion (pandas, SQLAlchemy) and KPI aggregation:
  - Average delay
  - P90 delay
  - On-time rate
- Create an interactive Streamlit dashboard visualizing route reliability by hour and highlighting worst-performing lines.

## Repository Structure
- `data/` – raw and processed datasets (not versioned in git once .gitignore is added).
- `notebooks/` – exploratory data analysis and experiments.
- `src/` – Python package for data ingestion and KPI computation.
- `dashboard/` – Streamlit app for interactive visualization.

## How to run the project

### 1. Create a virtual environment and install dependencies

```bash
python -m venv .venv
source .venv/bin/activate  # macOS / Linux
# On Windows (PowerShell):
# .venv\\Scripts\\Activate.ps1

pip install -r requirements.txt
```

### 2. Generate realistic delay data from real GTFS schedules

This project can create a synthetic delay dataset using **real GTFS schedules** (routes, trips, stops) and **plausible random delays**.

1. Download the GTFS static feed for the network you care about (for example, GTT Torino) into `data/raw/gtt_gtfs.zip`:

   ```bash
   mkdir -p data/raw
   curl -L -o data/raw/gtt_gtfs.zip "https://www.gtt.to.it/open_data/gtt_gtfs.zip"
   ```

2. Generate a synthetic delays file based on those schedules:

   ```bash
   python -m src.gtfs_to_delays \
     --gtfs-zip data/raw/gtt_gtfs.zip \
     --output-csv data/raw/delays.csv \
     --max-rows 20000
   ```

This writes `data/raw/delays.csv` with the schema expected by the ingestion step:

- `timestamp` – observation time (derived from scheduled departure).
- `route_id` – identifier of the route/line (from GTFS `routes.txt`).
- `trip_id` – trip identifier (from GTFS `trips.txt`).
- `stop_id` – stop identifier (from GTFS `stop_times.txt`).
- `scheduled_departure` – scheduled departure datetime (from GTFS times).
- `actual_departure` – scheduled time plus a synthetic delay in minutes.
- `mode` – transport mode inferred from GTFS `route_type` (bus, tram, metro, etc.).

If you already have a real delays dataset, you can also bypass this step and write your own `data/raw/delays.csv` matching the same schema.

### 3. Run data ingestion

From the repository root:

```bash
python -m src.data_ingestion \
  --input data/raw/delays.csv \
  --output data/processed/delays.parquet
```

This will write a cleaned dataset to `data/processed/` and compute `delay_minutes`, `date`, and `hour` helper columns.

### 4. Compute KPIs (optional CLI summary)

```bash
python -m src.kpi_aggregation \
  --input data/processed/delays.parquet \
  --output-route-kpis data/processed/route_kpis.csv
```

This prints the worst-performing routes by average delay and writes a per-route KPI summary to `data/processed/route_kpis.csv`.

### 5. Run the Streamlit dashboard

```bash
streamlit run dashboard/app.py
```

Then open the URL that Streamlit prints (usually `http://localhost:8501`). The dashboard lets you:

- Filter by route, mode, and date range.
- See overall KPIs (average delay, P90 delay, on-time rate) for the selection.
- Inspect per-route KPIs and an hourly delay profile for a chosen route.
# public-transport-delay-analysis
