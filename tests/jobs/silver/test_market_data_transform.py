import shutil
from pathlib import Path

import pytest

from jobs.silver.market_data_transform import MarketDataTransform


@pytest.fixture
def job():
    return MarketDataTransform(config_file_name='test-transformation.yaml')


def test_run(job):
    if Path(job.silver_location).exists():
        shutil.rmtree(job.silver_location)

    job.run(from_date=20261007, to_date=20261008)
    actual_output = job.spark.read.format('delta').load(job.silver_location)
    expected_columns = ['ticker_and_country', 'ticker', 'country', 'date', 'open', 'high', 'low', 'close', 'volume',
                        'source_name', 'ingest_timestamp', 'ingest_execution_id', 'execution_id', 'business_date']

    assert actual_output.count() == 25
    assert actual_output.columns == expected_columns
