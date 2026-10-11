import logging

from src.jobs.bronze.ingest_job_base import IngestJobBase

logger = logging.getLogger(__name__)

class MarketDataIngest(IngestJobBase):

    def __init__(self, config_file_name='application.yaml'):
        super().__init__(config_file_name=config_file_name)
        self.source_name = self.config['app.market-data.source-name']
        landing_relative_location = self.config['app.market-data.landing-location']
        bronze_relative_location = self.config['app.market-data.bronze-location']
        self.landing_location = f'{self.root_dir}/{landing_relative_location}'
        self.bronze_location = f'{self.root_dir}/{bronze_relative_location}'

    def get_landing_location(self) -> str:
        return self.landing_location

    def get_bronze_location(self) -> str:
        return self.bronze_location

    def get_source_name(self) -> str:
        return self.source_name

    def get_file_name_for_date(self, date: str) -> str:
        return f"{date}_d.txt"
