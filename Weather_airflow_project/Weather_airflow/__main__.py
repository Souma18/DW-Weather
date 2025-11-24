# TODO: Update the main function to your needs or remove it.

import sys
import os
from dotenv import load_dotenv


SCRIPTS_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "scripts"))
load_dotenv(os.path.join(SCRIPTS_DIR, ".env"))
PROJECT_ROOT = os.path.abspath(SCRIPTS_DIR)
sys.path.append(PROJECT_ROOT)
from clean.controller import process_etl
from load.load_to_bigquery import WeatherLoadToBigQuery
from extract.extractor import run
from transform.transform import run_transform
def main() -> None:
    process_etl()


if __name__ == "__main__":
    main()
