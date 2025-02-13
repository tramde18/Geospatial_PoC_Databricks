# Databricks notebook source
import logging
import sys
import os
 
current = os.path.dirname(os.path.abspath("__file__"))
parent = os.path.dirname(current)
sys.path.append(parent)

import geomodules.config as cfg

logger = logging.getLogger()
logger.setLevel("INFO")

# define query parameters
full_input_path = f"{cfg.CATALOG_NAME}.{cfg.SCHEMA_NAME}.{cfg.SILVER_TABLE_ROADS_LGA_TELEMATICS_JOINED}"
full_output_path = f'{cfg.CATALOG_NAME}.{cfg.SCHEMA_NAME}.{cfg.GOLD_ROADS_MEAN_SPEED}'

# define, execute query and save as view
logger.info(f"Saving gold ouput to {full_output_path}")
sql_query = f"""
CREATE OR REPLACE VIEW {full_output_path} AS
SELECT road_name, road_type, LGA_NAME23 as lga, state, AUS_NAME21 as country
, AVG(med_speed) AS average_median_speed, AVG(speed) AS posted_speed_limit, 
       AVG(med_speed - speed) AS speed_difference, h3_index AS h3_index, roads_geom_0 as geometry
FROM {full_input_path}
GROUP BY road_name, road_type, LGA_NAME23, state, AUS_NAME21, roads_geom_0, h3_index
HAVING AVG(speed) > 0 -- Ensures we only include rows where a speed limit is posted
ORDER BY speed_difference DESC;
"""
spark.sql(sql_query)

logger.info(f"Successfully saved gold ouput to {full_output_path}")

# grant access to view
spark.sql(f"GRANT SELECT ON VIEW {full_output_path} TO developers")
