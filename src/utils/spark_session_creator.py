from pathlib import Path

from pyspark.sql import SparkSession


class SparkSessionCreator:

    def __init__(self):
        self.spark = None

    def create_spark_session(self) -> SparkSession:

        root_dir = Path(__file__).resolve().parent.parent.parent

        spark = SparkSession.builder \
            .master("local[*]") \
            .appName("cibc-exercise") \
            .config("spark.driver.host", "127.0.0.1") \
            .config("spark.driver.bindAddress", "127.0.0.1") \
            .config("spark.jars", f"{root_dir}/jars/delta-storage-3.0.0.jar,{root_dir}/jars/delta-spark_2.12-3.0.0.jar") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.databricks.delta.autoOptimize.autoCompact", "true") \
            .config("spark.databricks.delta.autoOptimize.optimizeWrite", "true") \
            .config("spark.sql.sources.partitionOverwriteMode", "DYNAMIC") \
            .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
            .getOrCreate()

        spark.sparkContext.setLogLevel("WARN")

        return spark