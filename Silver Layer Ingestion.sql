-- Databricks notebook source
-- MAGIC %python
-- MAGIC %pip install databricks-mosaic
-- MAGIC import mosaic as mos
-- MAGIC mos.enable_mosaic(spark, dbutils)

-- COMMAND ----------

USE liliana_tang_demo_db;

-- COMMAND ----------

create table if not exists neighbourhoods_silver as (
  select
    type,
    feature.properties as properties,
    st_astext(st_geomfromgeojson(to_json(feature.geometry))) as geometry 
  from (
    select
      type,
      explode(features) as feature
    from taxi_zones
  )
);

-- COMMAND ----------

create table if not exists trips_silver as (
  select 
    trip_distance,
    pickup_datetime,
    dropoff_datetime,
    st_astext(st_point(pickup_longitude, pickup_latitude)) as pickup_geom,
    st_astext(st_point(dropoff_longitude, dropoff_latitude)) as dropoff_geom,
    total_amount
  from nyctaxi_yellow
)
