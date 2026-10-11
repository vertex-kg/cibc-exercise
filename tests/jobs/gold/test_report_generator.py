from datetime import datetime

import pytest
import pyspark.sql.functions as F

from jobs.gold.report_generator import ReportGenerator
from chispa.dataframe_comparer import assert_df_equality


@pytest.fixture
def job():
    return ReportGenerator(config_file_name='test-reporting.yaml')


def test_net_positions_report(job):
    current_date = datetime.today().strftime('%Y-%m-%d')
    actual_df = (job.net_positions_report()
                 .sort('book', 'symbol'))

    expected_data = [
        ('Book_A', 'AAPL', 300, 224.100000, current_date),
        ('Book_A', 'AMZN', 120, 185.400000, current_date),
        ('Book_A', 'ETHUSD', 100, 2450.000000, current_date),
        ('Book_A', 'GBPUSD', 250000, 1.301500, current_date),
        ('Book_A', 'GOOG', 150, 168.500000, current_date),
        ('Book_A', 'NVDA', 500, 127.800000, current_date),
        ('Book_B', 'MSFT', 92, 412.000000, current_date),
        ('Book_B', 'TSLA', -50, 0.000000, current_date),
        ('Book_B', 'US10Y', -100, 0.000000, current_date),
        ('Book_C', 'GC1!', 15, 2642.000000, current_date)
    ]
    expected_schema = ('book string, symbol string, net_position_size long, '
                       'average_entry_price double, as_of_date string')

    expected_df = (job.spark.createDataFrame(expected_data, expected_schema)
                   .sort('book', 'symbol')
                   .withColumn("net_position_size", F.col('net_position_size').cast('decimal(38,0)'))
                   .withColumn("average_entry_price", F.col('average_entry_price').cast('decimal(38,6)'))
                   .withColumn("as_of_date", F.col('as_of_date').cast('date')))

    assert_df_equality(actual_df, expected_df)


def test_daily_pnl_report(job):

    actual_df = (job.daily_pnl_report(as_of_date=20261009)
                 .sort('book', 'symbol'))

    expected_data = [
        ('Book_A', 'AAPL', 34023.00, '2026-10-09'),
        ('Book_A', 'AMZN', 8846.41, '2026-10-09'),
        ('Book_A', 'ETHUSD', 2191.00, '2026-10-09'),
        ('Book_A', 'GBPUSD', 4982.50, '2026-10-09'),
        ('Book_A', 'GOOG', 26991.75, '2026-10-09'),
        ('Book_A', 'NVDA', 54085.00, '2026-10-09'),
        ('Book_B', 'MSFT', 10885.90, '2026-10-09'),
        ('Book_B', 'TSLA', 0.00, '2026-10-09'),
        ('Book_B', 'US10Y', 0.00, '2026-10-09'),
        ('Book_C', 'GC1!', 23614.50, '2026-10-09'),
    ]
    expected_schema = 'book string, symbol string, total_pnl double, as_of_date string'

    expected_df = (job.spark.createDataFrame(expected_data, expected_schema)
                   .sort('book', 'symbol')
                   .withColumn("total_pnl", F.col('total_pnl').cast('decimal(38,2)'))
                   .withColumn("as_of_date", F.col('as_of_date').cast('date')))

    assert_df_equality(actual_df, expected_df)
