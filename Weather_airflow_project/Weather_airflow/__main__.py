
import sys
import os

from dotenv import load_dotenv
SCRIPTS_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "scripts"))
load_dotenv(os.path.join(SCRIPTS_DIR, ".env"))
PROJECT_ROOT = os.path.abspath(SCRIPTS_DIR)
sys.path.append(PROJECT_ROOT)
from transform.transform import run_transform
from clean.clean_process import run_clean_and_insert_all
from extract.extractor import run
def main() -> None:
    # run()
    # run_clean_and_insert_all()
    run_transform()


if __name__ == "__main__":
    main()
