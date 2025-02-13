# Databricks notebook source
"""
    WIDGET PARAMETERS
    GeoData: String -> Can be `roads`, `telematics`, 'lga'
    H3 Resolution: String -> Any H3 Resolution of choice, will be converted to Int within the code
"""

import sys
import os
 
current = os.path.dirname(os.path.abspath("__file__"))
parent = os.path.dirname(current)
sys.path.append(parent)

"""
    Generic Functionality that Process the ff. Geometries (Polygon, MultiLineString, Point)
"""
from geomodules.pymosaic import PyMosaic
from geomodules.logger import Logger
import geomodules.config as cfg

from mosaic import st_geometrytype
from mosaic import st_intersects
from py4j.protocol import Py4JJavaError
import pyspark.sql.functions as F

try:
    h3_resolution = int(dbutils.widgets.get("H3 Resolution"))
    geo_asset = dbutils.widgets.get("Geo Asset").lower()
except Py4JJavaError:
    dbutils.widgets.text("H3 Resolution", "10")
    dbutils.widgets.text("Geo Asset", "")
    raise ValueError("Need to define parameters for H3 and geo_asset")

pymos = PyMosaic(spark, dbutils)

bronze_table_name = f"bronze_{geo_asset}"
silver_table_name = f"silver_{geo_asset}"
full_bronze_tbl_path = f"{cfg.CATALOG_NAME}.{cfg.SCHEMA_NAME}.{bronze_table_name}"
full_silver_tbl_path = f"{cfg.CATALOG_NAME}.{cfg.SCHEMA_NAME}.{silver_table_name}"

Logger.displayInfo(f"Reading {full_bronze_tbl_path}...")
df_geo_bronze = (
    spark
    .read
    .table(full_bronze_tbl_path)
    .withColumn("geom_0_srid", F.lit(cfg.SRID))
    .withColumn("geom_type", st_geometrytype(F.col("geom_0")))
)

df_h3_indexed = (
    pymos.generateH3Index(
        df_geo_bronze
    ).withColumn('load_date', F.current_timestamp())
)

Logger.displayInfo(f"Writing H3 Indexed Data into {full_silver_tbl_path}...")
(
    df_h3_indexed
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(full_silver_tbl_path)
)
Logger.displayInfo(f'Successfully written H3 data into {full_silver_tbl_path}')

# COMMAND ----------

group = 'developers'

spark.sql(f"GRANT SELECT ON TABLE {cfg.CATALOG_NAME}.{cfg.SCHEMA_NAME}.{silver_table_name} TO {group}")
