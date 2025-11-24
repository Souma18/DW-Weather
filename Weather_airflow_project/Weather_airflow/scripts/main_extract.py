

from dotenv import load_dotenv
load_dotenv( ".env")
from extract.extractor import run
def main() -> None:
    run()


if __name__ == "__main__":
    main()
