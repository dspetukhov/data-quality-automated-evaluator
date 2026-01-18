import os
import sys
import json
from pathlib import Path
from utility import logging, read_source
from preprocess import make_preprocessing
from report import make_report


def main(config: dict) -> None:
    """
    Main function to execute the data quality evaluation pipeline.
    Requires `config.json` correct preset.

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

    # Check if the configuration file exists
    if os.path.exists("config.json"):
        with open("config.json") as file:
            config = json.load(file)
    else:
        logging.error("Configuration file wasn't found")

    # Proceed if configuration was loaded and contains `source`
    if config.get("source"):
        source_data = read_source(config.get("source"))
        # Preprocess data
        df, metadata = make_preprocessing(source_data, config)
        # Generate a report if preprocessing was successful
        make_report(df, metadata, config)


if __name__ == "__main__":

    if len(sys.argv) <= 2:
        config = {}
        if len(sys.argv) == 2:
            config_file_path = Path(sys.argv[1])
            if config_file_path.exists() and config_file_path.is_file():
                try:
                    with open(config_file_path) as file:
                        config = json.load(file)
                except Exception as e:
                    logging.error(
                        f"Error reading provided {config_file_path}: {e}"
                    )

        main(config)

    else:
        logging.warning("Usage: python main.py <config_file_path>")
        sys.exit(1)
