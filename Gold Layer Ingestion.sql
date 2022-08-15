-- Databricks notebook source
-- MAGIC %python
-- MAGIC %pip install databricks-mosaic
-- MAGIC import mosaic as mos
-- MAGIC mos.enable_mosaic(spark, dbutils)
-- MAGIC from mosaic import MosaicFrame

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Spatial Joins

-- COMMAND ----------

-- MAGIC %md
-- MAGIC We can use Mosaic to perform spatial joins both with and without Mosaic indexing strategies. </br>
-- MAGIC Indexing is very important when handling very different geometries both in size and in shape (ie. number of vertices). </br>

-- COMMAND ----------

USE liliana_tang_demo_db;

-- COMMAND ----------

create or replace temp view tripsWithIndex as (
  select 
    *,
    point_index_geom(pickup_geom, 9) as pickup_h3,
    point_index_geom(dropoff_geom, 9) as dropoff_h3,
    st_makeline(array(pickup_geom, dropoff_geom)) as trip_line
  from trips_silver
)

-- COMMAND ----------

create or replace temp view neighbourhoodsWithIndex as (
  select 
    *,
    mosaic_explode(geometry, 9) as mosaic_index
  from neighbourhoods_silver
)

-- COMMAND ----------

create or replace temp view withPickupZone as (
  select 
     trip_distance, pickup_geom, dropoff_geom, pickup_h3, dropoff_h3, pickup_zone, trip_line
  from tripsWithIndex
  join (
    select 
      properties.zone as pickup_zone,
      mosaic_index
    from neighbourhoodsWithIndex
  )
  on mosaic_index.index_id == pickup_h3
  where mosaic_index.is_core or st_contains(mosaic_index.wkb, pickup_geom)
);

-- COMMAND ----------

create table if not exists gold_taxi_trips as (
  select 
     trip_distance, pickup_geom, dropoff_geom, pickup_h3, dropoff_h3, pickup_zone, dropoff_zone, trip_line
  from withPickupZone
  join (
    select 
      properties.zone as dropoff_zone,
      mosaic_index
    from neighbourhoodsWithIndex
  )
  on mosaic_index.index_id == dropoff_h3
  where mosaic_index.is_core or st_contains(mosaic_index.wkb, dropoff_geom)
);
