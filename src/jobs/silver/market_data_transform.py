import logging

from pyspark.sql import functions as F, DataFrame
from pyspark.sql.functions import col, lit, split

from jobs.silver.transform_job_base import TransformJobBase

logger = logging.getLogger(__name__)


class MarketDataTransform(TransformJobBase):

    def __init__(self, config_file_name='application.yaml'):
        super().__init__(config_file_name=config_file_name)
        bronze_relative_location = self.config['app.market-data.bronze-location']
        silver_relative_location = self.config['app.market-data.silver-location']
        corrupt_relative_location = self.config['app.market-data.corrupt-location']
        self.bronze_location = f'{self.root_dir}/{bronze_relative_location}'
        self.silver_location = f'{self.root_dir}/{silver_relative_location}'
        self.corrupt_location = f'{self.root_dir}/{corrupt_relative_location}'

    def run(self, from_date: int, to_date: int):

        md_bronze_df = self.read_input_data(from_date=from_date, to_date=to_date)

        good_records_df = (md_bronze_df
        .filter(col("_is_good_record").eqNullSafe(True))
        .drop("_is_good_record"))

        standardised_df = self.enforce_schema(good_records_df)

        good_records_count = standardised_df.count()

        duplicates_count = (standardised_df
                            .groupBy('ticker_and_country', 'date')
                            .count()
                            .filter('count > 1')
                            .count())

        self.persist_output(standardised_df)

        corrupt_records_count = self.handle_corrupt_records(md_bronze_df, self.corrupt_location)

        self.collect_stats('market_data_transform', from_date, to_date,
                           self.silver_location, good_records_count, corrupt_records_count, duplicates_count)

        logger.info(f'Market Data Transform Complete')

    def persist_output(self, input_df: DataFrame):
        (input_df
         .dropDuplicates(['ticker_and_country', 'date'])
         .write
         .partitionBy("business_date")
         .mode("overwrite")
         .format("delta")
         .save(self.silver_location))

    def enforce_schema(self, input_df: DataFrame) -> DataFrame:
        return (input_df
        .select(
            col('<TICKER>').cast('string').alias('ticker_and_country'),
            split(col('<TICKER>'), '\\.', 2).getItem(0).alias('ticker'),
            split(col('<TICKER>'), '\\.', 2).getItem(1).alias('country'),
            F.to_date(col('<DATE>'), 'yyyyMMdd').cast('date').alias('date'),
            col('<OPEN>').cast('decimal(38,6)').alias('open'),
            col('<HIGH>').cast('decimal(38,6)').alias('high'),
            col('<LOW>').cast('decimal(38,6)').alias('low'),
            col('<CLOSE>').cast('decimal(38,6)').alias('close'),
            col('<VOL>').cast('long').alias('volume'),
            col('source_name').cast('string'),
            col('ingest_timestamp').cast('timestamp'),
            col('execution_id').cast('string').alias('ingest_execution_id'),
            lit(self.execution_id).cast('string').alias('execution_id'),
            col('business_date').cast('string').alias('business_date')))

    def read_input_data(self, from_date: int, to_date: int):
        good_record_condition = self.get_good_record_condition()

        return (self.spark.read
                .format('delta')
                .load(self.bronze_location)
                .filter(f"business_date >= {from_date} and business_date <= {to_date}")
                .withColumn("_is_good_record", good_record_condition))

    def get_good_record_condition(self):
        return (F.regexp(col("<TICKER>"), lit(r"[A-Z0-9\^\.]+")) &
                F.to_date(col("<DATE>"), "yyyyMMdd").isNotNull() &
                col("<OPEN>").cast("decimal").isNotNull() &
                col("<HIGH>").cast("decimal").isNotNull() &
                col("<LOW>").cast("decimal").isNotNull() &
                col("<CLOSE>").cast("decimal").isNotNull() &
                col("<VOL>").cast("long").isNotNull())
