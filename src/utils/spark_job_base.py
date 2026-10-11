import uuid
from pathlib import Path
import logging
import yaml

from utils.spark_session_creator import SparkSessionCreator

logging.basicConfig(level=logging.INFO)

class SparkJobBase:

    def __init__(self, config_file_name: str):
        self.root_dir = Path(__file__).resolve().parent.parent.parent
        session_creator = SparkSessionCreator()
        self.spark = session_creator.create_spark_session()
        self.execution_id = str(uuid.uuid4())

        config_file_path = self.root_dir / 'config' / config_file_name
        with open(config_file_path, "r") as config_file:
            self.config = yaml.safe_load(config_file)


