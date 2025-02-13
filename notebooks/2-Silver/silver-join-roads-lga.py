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

from geomodules.pymosaic import PyMosaic
from geomodules.logger import Logger
import geomodules.config as cfg

from mosaic import st_geometrytype
from mosaic import st_intersects
from py4j.protocol import Py4JJavaError
import pyspark.sql.functions as F

pymos = PyMosaic(spark, dbutils)

try:
    df_silver_lga = (
        spark
        .read
        .table(cfg.FULL_SILVER_LGA_PATH)
    )
except:
    dbutils.notebook.exit(f"{cfg.FULL_SILVER_LGA_PATH} delta table not found!!!, Exiting the process successfully...")

try: 
    df_silver_roads = (
        spark
        .read
        .table(cfg.FULL_SILVER_ROADS_PATH)
    )
except:
    dbutils.notebook.exit(f"{cfg.FULL_SILVER_ROADS_PATH} delta table not found!!!, Exiting the process successfully...")

#Get mapping to rename similar columns
Logger.displayInfo(f'Renaming similar columns...')
cols_mapping = pymos.mapDupCols(
                df_silver_roads,
                df_silver_lga,
                prefix=('roads_', 'lga_')
            )

df_silver_roads = (
    df_silver_roads
    .withColumnsRenamed(cols_mapping[0])
)

df_silver_lga = (
    df_silver_lga
    .withColumnsRenamed(cols_mapping[1])
)

Logger.displayInfo(f'Joining LGA and Roads Data...')
df_roads_lga_joined = (
    df_silver_roads
    .join(
        df_silver_lga, 
        st_intersects(
            df_silver_roads.roads_geom_0,
            df_silver_lga.lga_geom_0
        ),
        how='left'
    )
)

Logger.displayInfo(f"Writing Joined Data into {cfg.FULL_SILVER_ROADS_LGA_JOINED_PATH}...")
(
    df_roads_lga_joined
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(cfg.FULL_SILVER_ROADS_LGA_JOINED_PATH)
)
Logger.displayInfo(f'Successfully written H3 data into {cfg.FULL_SILVER_ROADS_LGA_JOINED_PATH}')

group = 'developers'

spark.sql(f"GRANT SELECT ON TABLE {cfg.FULL_SILVER_ROADS_LGA_JOINED_PATH} TO {group}")
