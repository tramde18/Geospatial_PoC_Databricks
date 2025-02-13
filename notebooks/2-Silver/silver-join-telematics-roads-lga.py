# Databricks notebook source
# import libraries and set up geo env
import sys
import os
import mlflow
import shortuuid

current = os.path.dirname(os.path.abspath("__file__"))
parent = os.path.dirname(current)
sys.path.append(parent)

from geomodules.pymosaic import PyMosaic
from geomodules.logger import Logger
import geomodules.config as cfg

from pyspark.databricks.sql import functions as dbf
from mosaic import st_geometrytype
from mosaic import st_intersects
from py4j.protocol import Py4JJavaError
from mosaic.models import SpatialKNN
import pyspark.sql.functions as F

pymos = PyMosaic(spark, dbutils)

# COMMAND ----------

# read silver tables
df_roads_lga_silver = spark.read.table(cfg.FULL_SILVER_ROADS_LGA_JOINED_PATH)
df_telematics_silver = spark.read.table(cfg.FULL_SILVER_TELEMATICS_PATH)

# initialize KNN class and set parameters
mlflow.set_registry_uri("databricks-uc")
experiment = mlflow.set_experiment(cfg.EXPERIMENT_PATH)
run_name = f"{cfg.RUN_ROOT_NAME}_{shortuuid.uuid()}"

Logger.displayInfo("Initializing KNN...")

mlflow.autolog(disable=False)
with mlflow.start_run(run_name=run_name) as run:
    knn = SpatialKNN()

    spark.sparkContext.setCheckpointDir(cfg.CHECKPOINT_DIR)
    knn.setUseTableCheckpoint(True)
    knn.setCheckpointTablePrefix(cfg.CHECKPOINT_TABLE_PREFIX)
    knn.model.cleanupCheckpoint

    knn.setDistanceThreshold(cfg.DISTANCE_THRESHOLD)

    knn.setIndexResolution(cfg.H3_RESOLUTION) # - e.g. H3 resolutions 0-15

    knn.setKNeighbours(cfg.K_NEIGHBORS)
    knn.setApproximate(cfg.APPROX)
    knn.setMaxIterations(cfg.MAX_ITER)
    knn.setEarlyStopIterations(cfg.EARLY_STOP_ITER)

    knn.setLandmarksFeatureCol(cfg.LANDMARKS_FEAT_COL)
    knn.setLandmarksRowID(cfg.LANDMARKS_ROW_ID) # id will be generated

    knn.setCandidatesDf(df_roads_lga_silver)
    knn.setCandidatesFeatureCol(cfg.CANDIDATES_FEAT_COL)
    knn.setCandidatesRowID(cfg.CANDIDATE_ROW_ID) # id will be generated

    # perform KNN join/prediction
    Logger.displayInfo("Performing KNN JOIN...")
    df_predict = knn.transform(df_telematics_silver)
    df_lga_roads_telematics = df_predict.where(df_predict.neighbour_number == 1)

    # log model parameters
    mlflow.log_params(knn.getParams())
    mlflow.log_metrics(knn.getMetrics())

# COMMAND ----------

# write df to table
(
    df_lga_roads_telematics
    .write
    .format("delta")
    .partitionBy(cfg.PARTITION_KEY)
    .mode("overwrite")
    .saveAsTable(cfg.FULL_SILVER_LGA_ROADS_TELEMATICS_JOINED_PATH)
)

group = 'developers'
spark.sql(f"GRANT SELECT ON TABLE {cfg.FULL_SILVER_LGA_ROADS_TELEMATICS_JOINED_PATH} TO {group}")
