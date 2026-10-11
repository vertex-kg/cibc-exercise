from pathlib import Path
from unittest import mock
from unittest.mock import Mock, call

import pytest
from pyspark.sql.types import StructField, StructType, StringType

from jobs.bronze.market_data_ingest import MarketDataIngest

@pytest.fixture
def job():
    return MarketDataIngest()

def test_run(job):
    job.ingest_single_file = Mock()
    job.run(from_date=20261006, to_date=20261010)

    actual_calls = job.ingest_single_file.mock_calls
    expected_calls = [
        call('20261006', '20261006_d.txt', f'{job.root_dir}/data/landing/stooq/20261006_d.txt'),
        call('20261007', '20261007_d.txt', f'{job.root_dir}/data/landing/stooq/20261007_d.txt'),
        call('20261008', '20261008_d.txt', f'{job.root_dir}/data/landing/stooq/20261008_d.txt'),
        call('20261009', '20261009_d.txt', f'{job.root_dir}/data/landing/stooq/20261009_d.txt'),
        call('20261010', '20261010_d.txt', f'{job.root_dir}/data/landing/stooq/20261010_d.txt'),
    ]
    assert actual_calls == expected_calls

def test_ingest_single_file(job):

    input_data = [
        ('US10Y', 'D', '20261006', '170000', '104.265625', '104.625', '104.140625', '104.5', '2218992', '0'),
        ('BTCUSD', 'D', '20261006', '000000', '85766.40', '86675.40', '85141.40', '85552.40', '46050', '0')
    ]

    schema = StructType([
        StructField('<TICKER>', StringType(), True),
        StructField('<PER>', StringType(), True),
        StructField('<DATE>', StringType(), True),
        StructField('<TIME>', StringType(), True),
        StructField('<OPEN>', StringType(), True),
        StructField('<HIGH>', StringType(), True),
        StructField('<LOW>', StringType(), True),
        StructField('<CLOSE>', StringType(), True),
        StructField('<VOL>', StringType(), True),
        StructField('<OPENINT>', StringType(), True),
    ])

    input_df = job.spark.createDataFrame(input_data, schema)
    input_df.show(truncate=False)
    job.read_file = Mock(return_value=input_df)
    job.persist_data = Mock()
    job.file_exists = Mock(return_value=True)

    job.ingest_single_file('20261010', '20261010_d.txt', '/parent/dir/20261010_d.txt')

    job.read_file.assert_called_with('/parent/dir/20261010_d.txt')
    calls = job.persist_data.mock_calls

    assert len(calls) == 1
    output_df = calls[0].args[0]

    expected_columns = ['<TICKER>', '<PER>', '<DATE>', '<TIME>', '<OPEN>', '<HIGH>', '<LOW>', '<CLOSE>', '<VOL>',
                        '<OPENINT>', '_corrupt_record', 'source_file_location', 'source_file_name', 'source_name',
                        'business_date', 'ingest_timestamp', 'execution_id']

    assert output_df.count() == 2
    assert output_df.columns == expected_columns
