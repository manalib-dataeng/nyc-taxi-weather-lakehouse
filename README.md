This project is about an end-to-end, production-style analytics platform that explores how weather drives NYC yellow-taxi demand and revenue — built on a medallion architecture.

How it works:
Ingest (Bronze) — 02_bronze_1 lands raw NYC taxi trips, zone lookup, and AccuWeather data into bronze_trips, bronze_zones, and bronze_weather.
Transform (Silver) — a serverless Spark Declarative Pipeline (taxi_pipeline) cleanses trips through built-in data-quality rules — invalid fares, timestamps, distances, and passenger counts are dropped — producing silver_trips and silver_weather.
Analyze (Gold) — joins taxi activity with weather at an hourly, zone-by-borough level, producing gold_demand_by_zone_hour and gold_revenue_by_weather, which buckets performance into dry / light rain / heavy rain days.
Monitor & Automate — 03_dq_check tracks the rejected rows through all the checks.
Visualize — the Taxi Demand vs. Weather dashboard turns it into shareable insights.
