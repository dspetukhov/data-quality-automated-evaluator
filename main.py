import sys
import json
from pathlib import Path
from utility import logging, exception_handler, read_source
from preprocess import make_preprocessing
from report import make_report


@exception_handler()
def main(config_file_path: str) -> None:
    """
    Main function to execute the data quality evaluation pipeline.
    Requires correct configuration preset.

    This pipeline includes the following steps:
      1. Checks if the configuration exists
         and loads it as a configuration using json library.
      2. Reads the data source as specified in the configuration.
      3. Preprocess data: applies filters and transformations (optional),
         aggregates data over dates.
      4. Generates a report as a markdown file based on the preprocessed data.

    Raises:
        SystemExit: If the configuration file is missing or cannot be parsed,
            or if the data source cannot be loaded.
    """
    # Try to load the configuration file
    config_file_path = Path(config_file_path)
    if config_file_path.exists() and config_file_path.is_file():
        with open(config_file_path) as file:
            config = json.load(file)
            logging.info(f"Configuration from {config_file_path} was loaded")
    else:
        config = {}
        logging.error("Configuration file wasn't found")

    # Proceed if configuration was loaded and contains `source`
    if config.get("source"):
        source_data = read_source(config["source"])
        # Preprocess data
        df, metadata = make_preprocessing(source_data, config)
        # Generate a report if preprocessing was successful
        make_report(df, metadata, config)


if __name__ == "__main__":
    if len(sys.argv) <= 2:
        config_file_path = "config.json"
        if len(sys.argv) == 2:
            config_file_path = sys.argv[1]

        main(config_file_path)

    else:
        logging.warning("Usage: python main.py <config_file_path>")
        sys.exit(1)
