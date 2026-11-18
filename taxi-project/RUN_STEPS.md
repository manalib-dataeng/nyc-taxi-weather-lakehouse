# NYC Taxi + Weather Lakehouse — How to Run This Project

End-to-end medallion pipeline (Bronze → Silver → Gold) built on Databricks with Unity Catalog, a Lakeflow Spark Declarative Pipeline (SDP), Databricks Lakeflow Jobs, and an AI/BI dashboard.

## Architecture at a glance

```
samples.nyctaxi.trips ─┐
taxi zone lookup ──────┼─▶ 02_bronze_1 ─▶ bronze_trips / bronze_zones / bronze_weather
samples.accuweather ───┤                      │
UC volume .raw ────────┘                      ▼
                            taxi_pipeline (SDP, serverless)
                            ├─ silver_trips  (streaming table + DQ expectations, violations dropped)
                            └─ silver_weather
                                     │
              ┌──────────────────────┼──────────────────────┐
              ▼                      ▼                      ▼
   gold_demand_by_zone_hour   gold_revenue_by_weather   03_dq_check
   (zone × hour demand)       (rain-bucket revenue)     (dq_metrics tables)
              └──────────────┬───────┘
                             ▼
              AI/BI dashboard “Taxi Demand vs. Weather”
```

All tables live in the Unity Catalog schema **`workspace.taxi_lakehouse`**.

## Repo layout

```
taxi-project/
├── RUN_STEPS.md                  ← you are here
├── notebooks/
│   ├── 02_bronze_1               (Bronze ingest notebook)
│   └── 03_dq_check               (DQ metrics notebook)
├── taxi_pipeline/
│   └── transformations/
│       ├── silver.sql            (silver_trips: streaming table + expectations)
│       ├── silver_weather.py     (silver_weather: Python flow)
│       └── gold.sql              (gold_demand_by_zone_hour, gold_revenue_by_weather)
└── dashboards/
    └── Taxi Demand vs. Weather.lvdash.json
```

---

## Prerequisites

- A Databricks workspace with **Unity Catalog** and **serverless compute** enabled.
- Permission to create tables/volumes in the target catalog and schema (default: `workspace.taxi_lakehouse`).
- The sample datasets `samples.nyctaxi.trips` and `samples.accuweather` (available in every workspace).
- Optional: raw taxi/weather files uploaded to a Unity Catalog volume (default: `workspace.taxi_lakehouse.raw`) if you want to ingest from files instead of the sample tables.

---

## Step 1 — Set up catalog, schema, and volume

In a notebook (or SQL editor), run:

```sql
CREATE CATALOG IF NOT EXISTS workspace;
CREATE SCHEMA IF NOT EXISTS workspace.taxi_lakehouse;
CREATE VOLUME IF NOT EXISTS workspace.taxi_lakehouse.raw;
```

Skip this if the schema already exists in your workspace.

## Step 2 — Import the notebooks

1. In the workspace, go to **Workspace → Import** (or open each file below and use *File → Import*).
2. Import `notebooks/02_bronze_1` and `notebooks/03_dq_check` from this repo into your user folder (e.g. `/Users/<you>/`).
3. Keep the folder names used by the orchestration job, or update the job task paths (Step 6) if you place them elsewhere.

## Step 3 — Create the Spark Declarative Pipeline (Silver + Gold)

1. In the workspace, create a new **ETL pipeline** (Lakeflow Spark Declarative Pipeline).
2. Name it `taxi_pipeline` (or any name; update the job in Step 6 accordingly).
3. Set the **destination catalog/schema** to `workspace.taxi_lakehouse`.
4. Enable **serverless**.
5. Add the three files from `taxi_pipeline/transformations/` as pipeline source files:
   - `silver.sql` — defines `silver_trips` as a **streaming table** with DQ expectations (valid fare, trip times, distance, passenger count; violating rows are dropped) reading from `bronze_trips` / `bronze_zones` via Auto CDC/streaming reads.
   - `silver_weather.py` — Python flow that cleans and conforms `bronze_weather` into `silver_weather`.
   - `gold.sql` — builds the two gold tables:
     - `gold_demand_by_zone_hour`: hourly trips, average fare, and revenue by zone/borough, joined to temperature and precipitation.
     - `gold_revenue_by_weather`: revenue bucketed by dry / light rain / heavy rain.
6. Click **Start** to run the pipeline. On the very first run use **Full refresh** so all streaming tables are built from scratch.

> Note: the pipeline reads from the bronze tables, so run Step 4 (bronze ingest) before the first pipeline run — or run the pipeline after the job in Step 6 has executed the bronze notebook.

## Step 4 — Load Bronze

Open the imported `02_bronze_1` notebook, **attach serverless compute**, and **Run all**. It:

- reads `samples.nyctaxi.trips` (and/or raw files from the UC volume) into `bronze_trips`,
- loads the taxi zone lookup into `bronze_zones`,
- reads `samples.accuweather` (and/or raw weather files) into `bronze_weather`.

## Step 5 — Run the pipeline and the DQ checks

1. **Pipeline:** trigger the `taxi_pipeline` update again (it picks up the bronze data written in Step 4) and wait for it to complete.
2. **DQ checks:** open the `03_dq_check` notebook and **Run all**. It writes the quality metrics tables:
   - `dq_metrics`
   - `dq_metrics_event_log`

## Step 6 — Orchestrate with a Lakeflow Job

Everything is chained in the job **`NYC_taxi_pipeline_job`** (job id `358731610978261`), scheduled **daily at 20:36 America/New_York**:

| # | Task | Type | Depends on |
| --- | --- | --- | --- |
| 1 | `notebook_02_bronze` | notebook `02_bronze_1` | — |
| 2 | `taxi_pipeline` | SDP pipeline | task 1 |
| 3 | `DQ_metrics` | notebook `03_dq_check` | task 2 |

To recreate it in your own workspace:

1. Go to **Jobs & Pipelines → Create job**.
2. Add the three tasks above in order, wiring the dependencies (`notebook_02_bronze` → `taxi_pipeline` → `DQ_metrics`).
3. Add a **daily schedule** (time of day of your choice) with timezone `America/New_York`.
4. Click **Run now** to trigger a full end-to-end run on demand.

## Step 7 — Explore results in the AI/BI dashboard

1. Import `dashboards/Taxi Demand vs. Weather.lvdash.json` via **Dashboards → Create dashboard → Import dashboard JSON** (or *File → Import*).
2. Point its datasets at the gold tables:
   - `workspace.taxi_lakehouse.gold_demand_by_zone_hour`
   - `workspace.taxi_lakehouse.gold_revenue_by_weather`
3. Open the dashboard to see demand by zone/hour and revenue split by weather bucket.

---

## Quick verification checklist

After a successful run you should have, in `workspace.taxi_lakehouse`:

- [ ] `bronze_trips`, `bronze_zones`, `bronze_weather`
- [ ] `silver_trips` (streaming table), `silver_weather`
- [ ] `gold_demand_by_zone_hour`, `gold_revenue_by_weather`
- [ ] `dq_metrics`, `dq_metrics_event_log`
- [ ] A completed run of job `NYC_taxi_pipeline_job` (all 3 tasks green)
- [ ] The dashboard rendering data from both gold tables

## Troubleshooting

- **Pipeline fails with “table not found” (bronze tables):** run the bronze notebook (Step 4) first, then re-run the pipeline.
- **Streaming table needs a rebuild after schema changes:** use **Full refresh** on the pipeline (reprocesses from scratch).
- **DQ tables empty:** the `03_dq_check` notebook must run *after* the pipeline completes; check the job task dependencies.
- **No data in gold tables:** check `silver_trips` row counts — rows violating the DQ expectations (fare, trip duration, distance, passenger count) are intentionally dropped.
