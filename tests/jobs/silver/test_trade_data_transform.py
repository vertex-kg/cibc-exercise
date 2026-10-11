import shutil
from pathlib import Path

import pytest

from jobs.silver.trade_data_transform import TradeDataTransform


@pytest.fixture
def job():
    return TradeDataTransform(config_file_name='test-transformation.yaml')


def test_run(job):
    if Path(job.silver_location).exists():
        shutil.rmtree(job.silver_location)

    job.run(from_date=20261007, to_date=20261008)

    actual_output = job.spark.read.format('delta').load(job.silver_location)
    expected_columns = ['trade_id', 'trade_version', 'trade_date', 'book', 'execution_timestamp', 'updated_timestamp',
                        'asset_class', 'symbol', 'transaction_type', 'quantity', 'price', 'currency', 'counterparty',
                        'status', 'source_name', 'ingest_timestamp', 'ingest_execution_id', 'execution_id',
                        'business_date']

    assert actual_output.count() == 10
    assert actual_output.columns == expected_columns
