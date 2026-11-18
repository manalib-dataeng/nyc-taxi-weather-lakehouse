from pyspark import pipelines as dp   # older docs use: import dlt
from pyspark.sql import functions as F

@dp.table(name="silver_weather")
def silver_weather():
    raw = spark.read.table("workspace.taxi_lakehouse.bronze_weather")
    return (raw.select(F.explode(F.arrays_zip(
                "hourly.time","hourly.temperature_2m","hourly.precipitation")).alias("r"))
        .select(F.to_timestamp(F.col("r")["time"]).alias("hour_ts"),
                F.col("r")["temperature_2m"].alias("temp_c"),
                F.col("r")["precipitation"].alias("precip_mm")))