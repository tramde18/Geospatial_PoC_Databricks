# Databricks notebook source
import geopandas as gpd
from shapely import wkt

import sys
import os
 
current = os.path.dirname(os.path.abspath("__file__"))
parent = os.path.dirname(current)
sys.path.append(parent)

import geomodules.config as cfg

output_file_name = dbutils.widgets.get("Exported File Name")
gold_view = dbutils.widgets.get("Gold View")
gold_view_geometry_column = dbutils.widgets.get("Geometry Column of Gold View")
export_file_path = f'{cfg.EXPORT_FILE_PATH}{output_file_name}'

spark_df = spark.read.table(f'{cfg.CATALOG_NAME}.{cfg.SCHEMA_NAME}.{gold_view}')

pandas_df = spark_df.toPandas()
pandas_df[gold_view_geometry_column] = pandas_df[gold_view_geometry_column].apply(wkt.loads)

geopandas_df = gpd.GeoDataFrame(pandas_df, geometry=gold_view_geometry_column)

columns_with_h3 = [col for col in geopandas_df.columns if 'h3_index' in col]

for col in columns_with_h3:
    geopandas_df[col] = geopandas_df[col].astype(str)

geopandas_df.to_file(export_file_path, driver=cfg.FILE_EXPORT_DRIVER)
