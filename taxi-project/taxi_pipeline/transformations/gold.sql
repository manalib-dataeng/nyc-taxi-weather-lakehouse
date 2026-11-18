CREATE OR REFRESH MATERIALIZED VIEW gold_demand_by_zone_hour AS
SELECT date_trunc('hour', t.pickup_ts) AS hour_ts,
       z.Zone AS pickup_zone, z.Borough,
       COUNT(*) AS trips,
       ROUND(AVG(t.fare_amount),2) AS avg_fare,
       ROUND(SUM(t.total_amount),2) AS revenue,
       MAX(w.temp_c) AS temp_c, MAX(w.precip_mm) AS precip_mm
FROM silver_trips t
LEFT JOIN workspace.taxi_lakehouse.bronze_zones z ON t.PULocationID = z.LocationID
LEFT JOIN silver_weather w ON date_trunc('hour', t.pickup_ts) = w.hour_ts
GROUP BY 1,2,3;

CREATE OR REFRESH MATERIALIZED VIEW gold_revenue_by_weather AS
SELECT CASE WHEN precip_mm = 0 THEN 'dry'
            WHEN precip_mm < 2.5 THEN 'light_rain' ELSE 'heavy_rain' END AS rain_bucket,
       SUM(trips) AS trips, ROUND(SUM(revenue),2) AS revenue,
       ROUND(AVG(avg_fare),2) AS avg_fare
FROM gold_demand_by_zone_hour
GROUP BY 1;