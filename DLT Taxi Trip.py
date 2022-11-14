# Databricks notebook source
# MAGIC %pip install databricks-mosaic

# COMMAND ----------

import mosaic as mos
mos.enable_mosaic(spark, dbutils)
import requests
import pathlib
import dlt
from pyspark.sql.functions import *
from mosaic import MosaicFrame

# COMMAND ----------

raw_path = f"dbfs:/liliana_tang_demos"
raw_taxi_zones_path = f"{raw_path}/taxi_zones"
def download_neighborhood_zone_data():
  user_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()  
  taxi_zones_url = 'https://data.cityofnewyork.us/api/geospatial/d3c5-ddgc?method=export&format=GeoJSON'
  # The DBFS file system is mounted under /dbfs/ directory on Databricks cluster nodes
  local_taxi_zones_path = pathlib.Path(raw_taxi_zones_path.replace('dbfs:/', '/dbfs/'))
  local_taxi_zones_path.mkdir(parents=True, exist_ok=True)
  req = requests.get(taxi_zones_url)
  with open(local_taxi_zones_path / f'nyc_taxi_zones.geojson', 'wb') as f:
    f.write(req.content)

# COMMAND ----------

# MAGIC %md
# MAGIC # 1. Bronze Layer Ingestion

# COMMAND ----------

raw_taxi_trips_path = f"/databricks-datasets/nyctaxi/tables/nyctaxi_yellow"
@dlt.table
def taxi_trips_bronze_dlt():
  return (
    spark.table("delta.`/databricks-datasets/nyctaxi/tables/nyctaxi_yellow`")
  )

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. Silver Layer Ingestion

# COMMAND ----------

download_neighborhood_zone_data()
@dlt.table
def neighborhood_silver_dlt():
  return (
    spark.read
      .option("multiline", "true")
      .format("json")
      .load(raw_taxi_zones_path)
      .select("type", explode(col("features")).alias("feature"))
      .select("type", col("feature.properties").alias("properties"), to_json(col("feature.geometry")).alias("json_geometry"))
    .withColumn("geometry", mos.st_aswkt(mos.st_geomfromgeojson("json_geometry")))
  )


# COMMAND ----------

@dlt.table
def taxi_trips_silver_dlt():
    trips_bronze = dlt.read("taxi_trips_bronze_dlt")
    trips = (trips_bronze.drop("vendorId", "rateCodeId", "store_and_fwd_flag", "payment_type")
      .withColumn("pickup_geom", mos.st_astext(mos.st_point(col("pickup_longitude"), col("pickup_latitude"))))
      .withColumn("dropoff_geom", mos.st_astext(mos.st_point(col("dropoff_longitude"), col("dropoff_latitude")))))
    return trips

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. Gold Layer Ingestion

# COMMAND ----------

@dlt.table
def taxi_pickup_gold():
  trips = dlt.read("taxi_trips_silver_dlt")
  neighborhoods = dlt.read("neighborhood_silver_dlt")
  optimal_resolution = 9
  tripsWithIndex = (trips
                    .withColumn("pickup_h3", mos.point_index_geom(col("pickup_geom"), lit(optimal_resolution)))
                    .withColumn("dropoff_h3", mos.point_index_geom(col("dropoff_geom"), lit(optimal_resolution))))
  neighbourhoodsWithIndex = (neighborhoods
                           .withColumn("mosaic_index", mos.mosaic_explode(col("geometry"), lit(optimal_resolution)))
                           .drop("json_geometry", "geometry"))
  pickupNeighbourhoods = neighbourhoodsWithIndex.select(col("properties.zone").alias("pickup_zone"), col("mosaic_index"))
  withPickupZone = (tripsWithIndex.join(pickupNeighbourhoods, tripsWithIndex["pickup_h3"] == pickupNeighbourhoods["mosaic_index.index_id"])
                                .where(col("mosaic_index.is_core") | mos.st_contains(col("mosaic_index.wkb"), col("pickup_geom")))
                                .select("trip_distance", "pickup_geom", "pickup_zone", "dropoff_geom", "pickup_h3", "dropoff_h3"))
  return withPickupZone
  
