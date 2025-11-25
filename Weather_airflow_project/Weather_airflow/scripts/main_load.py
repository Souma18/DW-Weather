
from load.load_to_bigquery import WeatherLoadToBigQuery
def main() -> None:
    WeatherLoadToBigQuery().run()


if __name__ == "__main__":
    main()
