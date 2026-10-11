import os
import shutil
from pathlib import Path

import pytest

from jobs.bronze.market_data_ingest import MarketDataIngest


@pytest.fixture
def job():
    return MarketDataIngest(config_file_name='test-ingestion.yaml')

def test_get_file_name_for_date(job):
    assert job.get_file_name_for_date("20261010") == "20261010_d.txt"

def test_run(job):

    if Path(job.bronze_location).exists():
        shutil.rmtree(job.bronze_location)

    job.run(from_date=20261007, to_date=20261008)
    actual_output = job.spark.read.format('delta').load(job.bronze_location)
    expected_columns = ['<TICKER>', '<PER>', '<DATE>', '<TIME>', '<OPEN>', '<HIGH>', '<LOW>', '<CLOSE>', '<VOL>',
                        '<OPENINT>', '_corrupt_record', 'source_file_location', 'source_file_name', 'source_name',
                        'business_date', 'ingest_timestamp', 'execution_id']

    assert actual_output.count() == 25
    assert actual_output.columns == expected_columns