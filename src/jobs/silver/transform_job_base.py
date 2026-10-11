import logging
from datetime import datetime

from delta import DeltaTable
from pyspark.sql import functions as F, Window, DataFrame
from pyspark.sql.functions import col, lit

from utils.spark_job_base import SparkJobBase

logger = logging.getLogger(__name__)


class TransformJobBase(SparkJobBase):

    def __init__(self, config_file_name):
        super().__init__(config_file_name=config_file_name)
        relative_stats_location = self.config.get("app.silver-stats.location")
        self.stats_location = f'{self.root_dir}/{relative_stats_location}'

    def handle_corrupt_records(self, input_df: DataFrame, corrupt_location) -> int:
        corrupt_records_df = input_df.filter(col("_is_good_record").eqNullSafe(False))
        corrupt_records_count = corrupt_records_df.count()

        select_expressions = [col(column_name).cast('string') for column_name in corrupt_records_df.columns]

        (corrupt_records_df
         .select(*select_expressions)
         .write
         .partitionBy("business_date")
         .mode("overwrite")
         .format("delta")
         .save(corrupt_location))

        return corrupt_records_count

    def collect_stats(self, pipeline_name, from_date, to_date, output_location,
                      good_records_count, corrupt_records_count, duplicates_count):

        current_time = datetime.now()
        current_date = current_time.strftime("%Y%m%d")

        stats_data = [(self.execution_id,
                       pipeline_name,
                       current_time,
                       current_date,
                       f'{from_date}-{to_date}',
                       output_location,
                       good_records_count,
                       corrupt_records_count,
                       duplicates_count)]

        stats_schema = ('execution_id string, pipeline_name string, processing_time timestamp, '
                        'processing_date string, date_range string, output_location string, '
                        'good_records_count integer, corrupt_records_count integer, duplicates_count integer')

        stats_df = self.spark.createDataFrame(stats_data, stats_schema)

        (stats_df.write
         .partitionBy("processing_date")
         .mode("append")
         .format("delta")
         .save(self.stats_location))
