import logging
from abc import abstractmethod
from datetime import datetime
from pathlib import Path

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StringType

from utils.spark_job_base import SparkJobBase

logger = logging.getLogger(__name__)

class IngestJobBase(SparkJobBase):

    def __init__(self, config_file_name: str):
        super().__init__(config_file_name)
        relative_stats_location = self.config.get("app.ingest-stats.location")
        self.stats_location = f'{self.root_dir}/{relative_stats_location}'

    @abstractmethod
    def get_landing_location(self) -> str:
        pass

    @abstractmethod
    def get_bronze_location(self) -> str:
        pass

    @abstractmethod
    def get_source_name(self) -> str:
        pass

    @abstractmethod
    def get_file_name_for_date(self, date: str) -> str:
        pass

    def run(self, from_date: int, to_date: int):
        logger.info(f"Ingesting [{self.get_source_name()}] data from [{from_date}] to [{to_date}]")
        logger.info(f"Landing location: {self.get_landing_location()}")

        start_date = datetime.strptime(str(from_date), "%Y%m%d")
        end_date = datetime.strptime(str(to_date), "%Y%m%d")
        date_list = pd.date_range(start_date, end_date, freq='D')

        for date in date_list:
            str_date = date.strftime("%Y%m%d")
            file_name = self.get_file_name_for_date(str_date)
            file_path = f"{self.get_landing_location()}/{file_name}"
            self.ingest_single_file(str_date, file_name, file_path)

        logger.info(f"Finished ingesting [{self.get_source_name()}] data")

    def file_exists(self, file_path: str) -> bool:
        return Path(file_path).is_file()

    def ingest_single_file(self, date: str, file_name: str, file_path: str):
        if not self.file_exists(file_path):
            logger.error(f'File {file_path} does not exist')
            return

        logger.info(f'Ingesting file: {file_path}')
        df = self.read_file(file_path)

        existing_columns = df.columns
        for column_name in existing_columns:
            df = df.withColumn(column_name, col(column_name).cast(StringType()))

        if '_corrupt_record' not in df.columns:
            df = df.withColumn('_corrupt_record', lit(None).cast('string'))

        current_time = datetime.now()

        with_metadata_df = (df
                            .withColumn('source_file_location', lit(self.get_landing_location()))
                            .withColumn('source_file_name', lit(file_name))
                            .withColumn('source_name', lit(self.get_source_name()))
                            .withColumn('business_date', lit(date))
                            .withColumn('ingest_timestamp', lit(current_time))
                            .withColumn('execution_id', lit(self.execution_id)))

        self.persist_data(with_metadata_df)

        self.collect_stats(with_metadata_df, current_time, date, file_name)

    def persist_data(self, df: DataFrame):
        (df.write
         .partitionBy("business_date")
         .mode("overwrite")
         .format("delta")
         .option("mergeSchema", "true")
         .save(self.get_bronze_location()))

    def read_file(self, file_path: str) -> DataFrame:
        return (self.spark.read
              .option('header', 'true')
              .option('delimiter', ',')
              .option('mode', 'PERMISSIVE')
              .option('columnNameOfCorruptRecord', '_corrupt_record')
              .csv(file_path))

    def collect_stats(self, df: DataFrame, current_time, business_date, file_name):
        good_records_count = df.filter(col('_corrupt_record').isNull()).count()
        corrupt_records_count = df.filter(col('_corrupt_record').isNotNull()).count()
        stats_data = [(self.execution_id,
                       self.get_source_name(),
                       current_time,
                       business_date,
                       self.get_landing_location(),
                       file_name,
                       self.get_bronze_location(),
                       good_records_count,
                       corrupt_records_count)]

        stats_schema = ('execution_id string, source_name string, ingest_time timestamp, business_date string, '
                        'landing_location string, file_name string, bronze_location string, '
                        'good_records_count integer, corrupt_records_count integer')

        stats_df = self.spark.createDataFrame(stats_data, stats_schema)

        (stats_df.write
         .partitionBy("business_date")
         .mode("append")
         .format("delta")
         .save(self.stats_location))
