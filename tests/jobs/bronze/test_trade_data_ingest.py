import shutil
from pathlib import Path

import pytest

from jobs.bronze.trade_data_ingest import TradeDataIngest


@pytest.fixture
def job():
    return TradeDataIngest(config_file_name="test-ingestion.yaml")


def test_get_file_name_for_date(job):
    assert job.get_file_name_for_date("20261010") == "trades_20261010.csv"


def test_run(job):
    if Path(job.bronze_location).exists():
        shutil.rmtree(job.bronze_location)

    job.run(from_date=20261008, to_date=20261008)

    actual_output = job.spark.read.format('delta').load(job.bronze_location)
    expected_columns = ['Trade_ID', 'Trade_Version', 'Trade_Date', 'Book', 'Execution_Timestamp', 'Updated_Timestamp',
                        'Asset_Class', 'Symbol', 'Transaction_Type', 'Quantity', 'Price', 'Currency', 'Counterparty',
                        'Status', '_corrupt_record', 'source_file_location', 'source_file_name', 'source_name',
                        'business_date', 'ingest_timestamp', 'execution_id']

    assert actual_output.count() == 11
    assert actual_output.columns == expected_columns
