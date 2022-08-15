# Databricks notebook source
import requests
import pathlib

# COMMAND ----------

# MAGIC %sql
# MAGIC USE liliana_tang_demo_db;

# COMMAND ----------

def neighborhood_zones_bronze():
  user_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
  raw_path = f"dbfs:/liliana_tang_demos"
  raw_taxi_zones_path = f"{raw_path}/taxi_zones"
  print(f"The raw data will be stored in {raw_path}")
  # Download taxi zones GeoJSON
  taxi_zones_url = 'https://data.cityofnewyork.us/api/geospatial/d3c5-ddgc?method=export&format=GeoJSON'
  # The DBFS file system is mounted under /dbfs/ directory on Databricks cluster nodes
  local_taxi_zones_path = pathlib.Path(raw_taxi_zones_path.replace('dbfs:/', '/dbfs/'))
  local_taxi_zones_path.mkdir(parents=True, exist_ok=True)
  req = requests.get(taxi_zones_url)
  with open(local_taxi_zones_path / f'nyc_taxi_zones.geojson', 'wb') as f:
    f.write(req.content)
  spark.sql(f"drop table if exists neighborhood_zones_bronze;")
  spark.sql(
    f"""
       create table if not exists neighborhood_zones_bronze
       using json
       options (multiline = true)
       location "{raw_taxi_zones_path}";
       """
  )
neighborhood_zones_bronze()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS taxi_trips_bronze
# MAGIC USING DELTA
# MAGIC LOCATION "/databricks-datasets/nyctaxi/tables/nyctaxi_yellow"
