CREATE OR REFRESH STREAMING TABLE silver_trips (
  CONSTRAINT valid_fare      EXPECT (fare_amount >= 0)                         ON VIOLATION DROP ROW,
  CONSTRAINT valid_times     EXPECT (tpep_dropoff_datetime > tpep_pickup_datetime) ON VIOLATION DROP ROW,
  CONSTRAINT valid_distance  EXPECT (trip_distance > 0 AND trip_distance < 100) ON VIOLATION DROP ROW,
  CONSTRAINT valid_passengers EXPECT (passenger_count BETWEEN 1 AND 6)
) TBLPROPERTIES ('delta.feature.timestampNtz' = 'supported') AS
SELECT
  CAST(tpep_pickup_datetime AS TIMESTAMP)  AS pickup_ts,
  CAST(tpep_dropoff_datetime AS TIMESTAMP) AS dropoff_ts,
  tpep_pickup_datetime, tpep_dropoff_datetime,
  PULocationID, DOLocationID, passenger_count,
  trip_distance, fare_amount, total_amount
FROM STREAM(workspace.taxi_lakehouse.bronze_trips);