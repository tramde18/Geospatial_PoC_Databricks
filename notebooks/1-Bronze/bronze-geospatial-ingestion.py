# Databricks notebook source
import sys
import os
 
current = os.path.dirname(os.path.abspath("__file__"))
parent = os.path.dirname(current)
sys.path.append(parent)

from geomodules.logger import Logger
from geomodules.pymosaic import PyMosaic
import geomodules.config as cfg
import pyspark.sql.functions as F
from py4j.protocol import Py4JJavaError

pymos = PyMosaic(spark, dbutils)

try:
    bronze_table_name = dbutils.widgets.get("Bronze Table Name")
    source_file = dbutils.widgets.get("Source File")
except Py4JJavaError:
    dbutils.widgets.text("Bronze Table Name", "bronze_roads")
    dbutils.widgets.text("Source File", "roads.geojson")
    raise ValueError("Need to define parameters for Bronze Table Name or Source File")

source_file_abs_path = f'{cfg.SOURCE_RAW_DATA_PATH}{source_file}'
full_table_path = f"{cfg.CATALOG_NAME}.{cfg.SCHEMA_NAME}.{bronze_table_name}"

# load source file and load to delta bronze table 
df_geo_bronze = (
    spark
    .read
    .format(cfg.FILE_FORMAT) 
    .load(source_file_abs_path) 
    .withColumn('load_date', F.current_timestamp())
    .withColumn('file_source', F.input_file_name())
)

Logger.displayInfo(f"Writing {source_file_abs_path} into {full_table_path}...")
(
    df_geo_bronze
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(full_table_path)
)
Logger.displayInfo(f"Successfully written {source_file_abs_path} into {full_table_path}...")


# COMMAND ----------

group = 'developers'

spark.sql(f"GRANT SELECT ON TABLE {cfg.CATALOG_NAME}.{cfg.SCHEMA_NAME}.{bronze_table_name} TO {group}")
