# TODO: Update the main function to your needs or remove it.

import sys
import os

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), 'scripts'))
sys.path.append(PROJECT_ROOT)
from transform.transform import run_transform

def main() -> None:
    run_transform()


if __name__ == "__main__":
    main()
