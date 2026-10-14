import logging

from delta import DeltaTable
from pyspark.sql import functions as F, Window, DataFrame
from pyspark.sql.functions import col, lit

from jobs.silver.transform_job_base import TransformJobBase

logger = logging.getLogger(__name__)


class TradeDataTransform(TransformJobBase):

    def __init__(self, config_file_name='application.yaml'):
        super().__init__(config_file_name=config_file_name)
        bronze_relative_location = self.config['app.trade-data.bronze-location']
        silver_relative_location = self.config['app.trade-data.silver-location']
        corrupt_relative_location = self.config['app.trade-data.corrupt-location']
        self.bronze_location = f'{self.root_dir}/{bronze_relative_location}'
        self.silver_location = f'{self.root_dir}/{silver_relative_location}'
        self.corrupt_location = f'{self.root_dir}/{corrupt_relative_location}'

    def run(self, from_date: int, to_date: int):

        bronze_df = self.read_bronze_data(from_date=from_date, to_date=to_date)
        tagged_df = self.tag_corrupt_records(bronze_df)
        good_records_df = (tagged_df
                           .filter(col("_is_good_record").eqNullSafe(True))
                           .drop("_is_good_record"))
        good_records_count = good_records_df.count()

        standardised_df = self.enforce_schema(good_records_df)
        duplicates_count = self.get_duplicate_records_count(standardised_df)
        deduped_df = self.deduplicate_trades(standardised_df)

        self.persist_trades(deduped_df)

        corrupt_records_count = self.handle_corrupt_records(tagged_df, self.corrupt_location)

        self.collect_stats('trade_data_transform', from_date, to_date,
                           self.silver_location, good_records_count, corrupt_records_count, duplicates_count)

        logger.info(f'Trade Data Transform Complete')

    def persist_trades(self, input_df: DataFrame):
        if DeltaTable.isDeltaTable(self.spark, self.silver_location):
            trades_table = DeltaTable.forPath(self.spark, self.silver_location)
            (trades_table.alias('left')
             .merge(input_df.alias('right'), condition='left.trade_id = right.trade_id')
             .whenMatchedUpdateAll(condition='left.trade_version < right.trade_version')
             .whenNotMatchedInsertAll()
             .execute()
             )
        else:
            (input_df.write
             .partitionBy("business_date")
             .mode("overwrite")
             .format("delta")
             .save(self.silver_location))

    def deduplicate_trades(self, input_df: DataFrame) -> DataFrame:
        window_spec = Window.partitionBy("trade_id").orderBy(F.desc("trade_version"))
        return (input_df
                .withColumn("rank", F.row_number().over(window_spec))
                .filter("rank = 1")
                .drop("rank"))

    def enforce_schema(self, input_df: DataFrame) -> DataFrame:
        return (input_df.select(
            col('Trade_ID').cast('string').alias('trade_id'),
            col('Trade_Version').cast('integer').alias('trade_version'),
            F.to_date(col('Trade_Date'), 'yyyy-MM-dd').cast('date').alias('trade_date'),
            col('Book').cast('string').alias('book'),
            F.to_timestamp(col('Execution_Timestamp'), "yyyy-MM-dd'T'HH:mm:ss.SSSX").cast('timestamp').alias(
                'execution_timestamp'),
            F.to_timestamp(col('Updated_Timestamp'), "yyyy-MM-dd'T'HH:mm:ss.SSSX").cast('timestamp').alias(
                'updated_timestamp'),
            col('Asset_Class').cast('string').alias('asset_class'),
            col('Symbol').cast('string').alias('symbol'),
            col('Transaction_Type').cast('string').alias('transaction_type'),
            col('Quantity').cast('decimal(38,6)').alias('quantity'),
            col('Price').cast('decimal(38,6)').alias('price'),
            col('Currency').cast('string').alias('currency'),
            col('Counterparty').cast('string').alias('counterparty'),
            col('Status').cast('string').alias('status'),
            col('source_name').cast('string'),
            col('ingest_timestamp').cast('timestamp'),
            col('execution_id').cast('string').alias('ingest_execution_id'),
            lit(self.execution_id).cast('string').alias('execution_id'),
            col('business_date').cast('string').alias('business_date')))

    def get_duplicate_records_count(self, input_df: DataFrame):
        return (input_df
         .groupBy('trade_id', 'trade_version')
         .count()
         .filter('count > 1')
         .count())

    def tag_corrupt_records(self, input_df: DataFrame) -> DataFrame:
        good_record_condition = self.get_good_record_condition()
        return input_df.withColumn("_is_good_record", good_record_condition)

    def read_bronze_data(self, from_date, to_date) -> DataFrame:
        return (self.spark.read
                        .format('delta')
                        .load(self.bronze_location)
                        .filter(f"business_date >= {from_date} and business_date <= {to_date}"))

    def get_good_record_condition(self):
        good_record_condition = (F.col("Trade_ID").cast("string").isNotNull() &
                                 F.col("Trade_Version").cast("integer").isNotNull() &
                                 F.to_date(col("Trade_Date"), "yyyy-MM-dd").isNotNull() &
                                 F.regexp(col("Book"), lit(r"[a-zA-Z_]+")).eqNullSafe(True) &
                                 F.to_timestamp(col("Execution_Timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSX").isNotNull() &
                                 F.to_timestamp(col("Updated_Timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSX").isNotNull() &
                                 F.regexp(col("Asset_Class"), lit(r"[a-zA-Z_]+")).eqNullSafe(True) &
                                 F.col("Symbol").isNotNull() &
                                 F.regexp(col("Symbol"), lit(r"[A-Z0-9\^]+")).eqNullSafe(True) &
                                 F.regexp(col("Transaction_Type"), lit(r"BUY|SELL")).eqNullSafe(True) &
                                 F.col("Quantity").cast("decimal").isNotNull() &
                                 F.col("Price").cast("decimal").isNotNull() &
                                 F.regexp(col("Currency"), lit(r"[A-Z]+")).eqNullSafe(True) &
                                 F.regexp(col("Counterparty"), lit(r"[a-zA-Z0-9\s_]+")).eqNullSafe(True) &
                                 F.regexp(col("Status"), lit(r"[a-zA-Z_]+")).eqNullSafe(True))

        return good_record_condition
