from datetime import datetime

from utils.spark_job_base import SparkJobBase
from pyspark.sql import functions as F, DataFrame


class ReportGenerator(SparkJobBase):

    def __init__(self, config_file_name='application.yaml'):
        super().__init__(config_file_name=config_file_name)
        td_silver_rel_location = self.config['app.trade-data.silver-location']
        md_silver_rel_location = self.config['app.market-data.silver-location']
        self.trade_data_silver_location = f'{self.root_dir}/{td_silver_rel_location}'
        self.market_data_silver_location = f'{self.root_dir}/{md_silver_rel_location}'

    def net_positions_report(self) -> DataFrame:
        current_date = datetime.today().strftime('%Y-%m-%d')
        trades_df = (self.spark
                     .read
                     .format('delta')
                     .load(self.trade_data_silver_location)
                     .filter("status not in ('Cancelled', 'Pending', 'Rejected')"))

        return (trades_df
                        .withColumn('adjusted_quantity', F.expr("case when transaction_type = 'BUY' then quantity else -quantity end"))
                        .withColumn('amount', F.expr("case when transaction_type = 'BUY' then quantity * price else 0 end"))
                        .groupBy('book', 'symbol')
                        .agg(F.sum('adjusted_quantity').cast("decimal(38,0)").alias('net_position_size'),
                             (F.sum('amount') / F.sum('adjusted_quantity')).cast("decimal(38,6)").alias('average_entry_price'))
                        .withColumn('as_of_date', F.lit(current_date).cast('date'))
                        .sort('book', 'symbol'))

    def daily_pnl_report(self, as_of_date: int) -> DataFrame:
        trades_df = (self.spark
                     .read
                     .format('delta')
                     .load(self.trade_data_silver_location)
                     .filter("status not in ('Cancelled', 'Pending', 'Rejected')"))

        trades_df = (trades_df
                     .withColumn('as_of_date', F.to_date(F.lit(as_of_date).cast('string'), format='yyyyMMdd')))

        prices_df = (self.spark
                     .read
                     .format('delta')
                     .load(self.market_data_silver_location))

        joined_df = trades_df.alias('trades')._joinAsOf(
            prices_df.alias('prices'),
            leftAsOfColumn='as_of_date',
            rightAsOfColumn='date',
            on=F.expr("trades.symbol = prices.ticker"),
            how="left")

        return (joined_df
                        .withColumn('trade_pnl', F.expr("case when transaction_type = 'BUY' then (close - price) * quantity else 0 end"))
                        .groupBy('book', 'symbol')
                        .agg(F.sum('trade_pnl').cast("decimal(38,2)").alias('total_pnl'))
                        .sort('book', 'symbol')
                        .withColumn('as_of_date', F.to_date(F.lit(str(as_of_date)), format='yyyyMMdd')))
