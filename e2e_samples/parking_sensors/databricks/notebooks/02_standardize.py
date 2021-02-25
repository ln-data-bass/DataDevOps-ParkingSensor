# Databricks notebook source
dbutils.widgets.text("infilefolder", "", "In - Folder Path")
infilefolder = dbutils.widgets.get("infilefolder")

dbutils.widgets.text("loadid", "", "Load Id")
loadid = dbutils.widgets.get("loadid")

# COMMAND ----------

from applicationinsights import TelemetryClient
tc = TelemetryClient(dbutils.secrets.get(scope = "storage_scope", key = "appinsights_key"))

# COMMAND ----------

import os
import datetime

# For testing
# infilefolder = 'datalake/data/lnd/2019_03_11_01_38_00/'
load_id = loadid
loaded_on = datetime.datetime.now()
base_path = os.path.join('dbfs:/mnt/datalake/data/lnd/', infilefolder)
parkingbay_filepath = os.path.join(base_path, "MelbParkingBayData.json")
sensors_filepath = os.path.join(base_path, "MelbParkingSensorData.json")

# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit, col, to_timestamp
from pyspark.sql.types import (
    ArrayType, StructType, StructField, StringType, DoubleType)  # noqa: E501

def local_standardize_parking_bay(parkingbay_sdf: DataFrame, load_id, loaded_on):
    t_parkingbay_sdf = (
        parkingbay_sdf
        .withColumn("last_edit", to_timestamp("last_edit", "MM-dd-yyyy HH:mm:ss. SSS"))
        .select(
            col("bay_id").cast("int").alias("bay_id"),
            "last_edit",
            "marker_id",
            "meter_id",
            "rd_seg_dsc",
            col("rd_seg_id").cast("int").alias("rd_seg_id"),
            "the_geom",
            lit(load_id).alias("load_id"),
            lit(loaded_on).alias("loaded_on")
        )
    ).cache()
    # Data Validation
    good_records = t_parkingbay_sdf.filter(col("bay_id").isNotNull())
    bad_records = t_parkingbay_sdf.filter(col("bay_id").isNull())
    return good_records, bad_records

def local_standardize_sensordata(sensordata_sdf: DataFrame, load_id, loaded_on):
    t_sensordata_sdf = (
        sensordata_sdf
        .select(
            col("bay_id").cast("int").alias("bay_id"),
            "st_marker_id",
            col("lat").cast("float").alias("lat"),
            col("lon").cast("float").alias("lon"),
            "location",
            "status",
            lit(load_id).alias("load_id"),
            lit(loaded_on).alias("loaded_on")
        )
    ).cache()
    # Data Validation
    good_records = t_sensordata_sdf.filter(col("bay_id").isNotNull())
    bad_records = t_sensordata_sdf.filter(col("bay_id").isNull())
    return good_records, bad_records

# COMMAND ----------

import ddo_transform.standardize as s

# Retrieve schema
parkingbay_schema = s.get_schema("in_parkingbay_schema")
sensordata_schema = s.get_schema("in_sensordata_schema")

# Read data
parkingbay_sdf = spark.read\
  .schema(parkingbay_schema)\
  .option("badRecordsPath", os.path.join(base_path, "__corrupt", "MelbParkingBayData"))\
  .option("multiLine", True)\
  .json(parkingbay_filepath)
sensordata_sdf = spark.read\
  .schema(sensordata_schema)\
  .option("badRecordsPath", os.path.join(base_path, "__corrupt", "MelbParkingSensorData"))\
  .option("multiLine", True)\
  .json(sensors_filepath)


# Standardize
t_parkingbay_sdf, t_parkingbay_malformed_sdf = local_standardize_parking_bay(parkingbay_sdf, load_id, loaded_on)
t_sensordata_sdf, t_sensordata_malformed_sdf = local_standardize_sensordata(sensordata_sdf, load_id, loaded_on)

# Insert new rows
t_parkingbay_sdf.write.mode("append").insertInto("interim.parking_bay")
t_sensordata_sdf.write.mode("append").insertInto("interim.sensor")

# Insert bad rows
t_parkingbay_malformed_sdf.write.mode("append").insertInto("malformed.parking_bay")
t_sensordata_malformed_sdf.write.mode("append").insertInto("malformed.sensor")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Metrics

# COMMAND ----------

parkingbay_count = t_parkingbay_sdf.count()
sensordata_count = t_sensordata_sdf.count()
parkingbay_malformed_count = t_parkingbay_malformed_sdf.count()
sensordata_malformed_count = t_sensordata_malformed_sdf.count()

tc.track_event('Standardize : Completed load', 
               properties={'parkingbay_filepath': parkingbay_filepath, 
                           'sensors_filepath': sensors_filepath,
                           'load_id': load_id 
                          },
               measurements={'parkingbay_count': parkingbay_count,
                             'sensordata_count': sensordata_count,
                             'parkingbay_malformed_count': parkingbay_malformed_count,
                             'sensordata_malformed_count': sensordata_malformed_count
                            })
tc.flush()

# COMMAND ----------

dbutils.notebook.exit("success")
