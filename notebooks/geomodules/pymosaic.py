from geomodules.logger import Logger
from pyspark.databricks.sql import functions as dbf
import geomodules.config as cfg
import pyspark.sql.functions as F
import mosaic as mos


class PyMosaic:
    def __init__(self, spark, dbutils):
        self.spark = spark
        self.dbutils = dbutils
        self.log = Logger
        self.initGDAL()

    def initGDAL(self):
        self.log.displayInfo(f"Enableing GDAL...")
        mos.enable_mosaic(
            self.spark, 
            self.dbutils
        )
        mos.enable_gdal(
            self.spark
        )

    def generateH3Index(self, df):
        self.log.displayInfo(f"Applying H3 Indexing...")
        df = (
            df
            .withColumn(
                "h3_index",
                F.when(
                    F.col("geom_type").isin(["MULTILINESTRING", "LINESTRING"]),
                    dbf.h3_coverash3string(F.col("geom_0"), cfg.H3_RESOLUTION)
                )
                .when(
                    F.col("geom_type").isin(["MULTIPOLYGON", "POLYGON"]),
                    dbf.h3_try_polyfillash3string(F.col("geom_0"), cfg.H3_RESOLUTION)
                )
                .when(
                    F.col("geom_type") == "POINT",
                    F.split(dbf.h3_pointash3string(F.col("geom_0"), cfg.H3_RESOLUTION), ",")
                )
            )
        )
        return df

    def mapDupCols(self, df1, df2, prefix: set=('', ''), suffix: set=('', '')) -> list:
        self.log.displayInfo(f"Mapping duplicate columns...")
        same_columns = [col for col in df1.columns if col in df2.columns]
        df1_cols_map, df2_cols_map = {}, {}
    
        for col in same_columns:
            df1_cols_map[col] = prefix[0] + col + suffix[0]
            df2_cols_map[col] = prefix[1] + col + suffix[1]
        
        return [df1_cols_map, df2_cols_map]

        