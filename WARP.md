# WARP.md

This file provides guidance to WARP (warp.dev) when working with code in this repository.

## Repository overview

This is a Python data science / ML project analyzing live bus and tram delays for Torino’s GTT network. The main goals are to ingest GTFS-based data, compute delay-related KPIs (average delay, P90 delay, on-time rate), and expose them via a Streamlit dashboard for interactive exploration.

High-level layout (see also `README.md`):
- `src/` – Python package for data ingestion and KPI computation.
- `dashboard/` – Streamlit app for visualizing route reliability and worst-performing lines.
- `notebooks/` – Jupyter notebooks for exploratory data analysis and experiments (not production code).
- `data/` – Raw and processed datasets; treated as input/output, not as source.

## Key commands and workflows

The project uses a simple `requirements.txt` with pandas, SQLAlchemy, streamlit, and pyarrow. There is no formal build system or test runner configuration yet; the commands below use standard Python tooling and can be adapted if a different toolchain is introduced later.

### Python environment

Use a virtual environment when working on this project:

```bash path=null start=null
python -m venv .venv
source .venv/bin/activate  # macOS / Linux
# On Windows (PowerShell):
# .venv\\Scripts\\Activate.ps1
```

Install dependencies from `requirements.txt` into the active environment:

```bash path=null start=null
pip install -r requirements.txt
```

### Generating delays from GTFS schedules

To create a realistic delays dataset using real GTFS schedules (routes, trips, stops) and synthetic delays, run:

```bash path=null start=null
mkdir -p data/raw
curl -L -o data/raw/gtt_gtfs.zip "https://www.gtt.to.it/open_data/gtt_gtfs.zip"

python -m src.gtfs_to_delays \
  --gtfs-zip data/raw/gtt_gtfs.zip \
  --output-csv data/raw/delays.csv \
  --max-rows 20000
```

This produces `data/raw/delays.csv` with the schema expected by ingestion.

### Running data ingestion and KPI scripts

Library-style code lives under `src/`, which is structured as a top-level Python package. From the project root, you can run modules directly with `python -m`:

```bash path=null start=null
# Run the GTFS / delay data ingestion pipeline
python -m src.data_ingestion \
  --input data/raw/delays.csv \
  --output data/processed/delays.parquet

# Run KPI aggregation logic and write per-route KPIs
python -m src.kpi_aggregation \
  --input data/processed/delays.parquet \
  --output-route-kpis data/processed/route_kpis.csv
```

These entrypoints are intentionally simple so they can be composed in future orchestration (e.g., cron jobs, Airflow, or Make targets) without changing the code layout.

### Running the Streamlit dashboard

With Streamlit installed in your environment and a processed dataset available, launch the app from the project root with:

```bash path=null start=null
streamlit run dashboard/app.py
```

By default, Streamlit will open a local web UI in your browser. Use this during development to validate new KPIs and visualizations.

### Tests and linting

As of now there are no test files or linting/formatting configuration checked into the repo. When these are introduced, prefer conventional tooling and invocation patterns so they remain easy to discover:

- **Tests** – Recommended default is `pytest`:
  - Run the full test suite from the project root:
    ```bash path=null start=null
    pytest
    ```
  - Run a single test file or test function (Warp will often need this pattern):
    ```bash path=null start=null
    pytest path/to/test_file.py::TestClass::test_case_name
    ```
- **Linting / formatting** – If tools like `ruff`, `flake8`, or `black` are adopted, add their canonical commands here (e.g., `ruff src dashboard`), and prefer project-level configuration files (`pyproject.toml`, `ruff.toml`, etc.).

Until such tools are configured, focus on keeping `src/` importable and `dashboard/app.py` runnable without errors.

## Architecture and code organization

The project is organized around a simple end-to-end pipeline:

1. **Data ingestion (`src/data_ingestion.py`)**
   - Responsible for connecting to GTT GTFS feeds or other upstream data sources.
   - Should encapsulate all logic for fetching, cleaning, and storing delay data (e.g., into local files under `data/` or a database via SQLAlchemy).
   - Downstream code (KPIs, dashboard) should depend on the cleaned outputs of this module, not on raw feeds.

2. **KPI aggregation (`src/kpi_aggregation.py`)**
   - Consumes the outputs of data ingestion to compute metrics like average delay, P90 delay, and on-time rate.
   - This module is the right place for reusable business logic: shared functions or classes that compute metrics for different routes, time windows, or transport modes.
   - The dashboard should import from here rather than re-implementing KPI logic.

3. **Interactive dashboard (`dashboard/app.py`)**
   - Streamlit entrypoint for exploring delay KPIs by route, hour of day, and line performance.
   - Should remain a thin UI layer that:
     - Calls into `src.kpi_aggregation` for metric computation.
     - Reads data products (tables, views, or files) produced by `src.data_ingestion`.
     - Handles user interaction (selecting routes, time filters, etc.) and visualization only.

4. **Exploration vs. production (`notebooks/` vs. `src/`)**
   - Use `notebooks/` for ad-hoc exploration, prototyping metrics, or visualizations.
   - Once logic stabilizes, move it into `src/` (for ingestion / KPIs) or `dashboard/` (for UI) so it can be reused and tested.

5. **Data storage (`data/`)**
   - Acts as the boundary between code and data: raw dumps from GTT, intermediate tables, and derived KPI datasets.
   - This directory should not contain source code and is expected to grow large; it is usually excluded from version control via `.gitignore`.

## Guidance for future Warp instances

- When adding new ingestion sources or KPIs, keep `src/` as the single source of truth for data and metrics, and have the dashboard and notebooks import from there.
- Prefer adding explicit CLI entrypoints (e.g., small `if __name__ == "__main__":` blocks in `data_ingestion.py` and `kpi_aggregation.py`) instead of scattering one-off scripts under `data/`.
- If you introduce new tooling (tests, linters, build system), update the **Key commands and workflows** section so subsequent Warp sessions can invoke them reliably.