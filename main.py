import os
import json
from polars import LazyFrame, DataFrame
from utility import logging, read_source
from preprocessing import make_preprocessing
from report import make_report


def main():
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
            try:
                config = json.load(file)
            except json.JSONDecodeError as e:
                logging.error(f"Error loading configuration file: {e}")
                config = None

        # Proceed if configuration was loaded and contains a `source`
        if config and config.get("source"):
            source_data = read_source(config["source"])
            if isinstance(source_data, LazyFrame):
                # Preprocess data
                df, metadata = make_preprocessing(source_data, config)
                # Generate a report if preprocessing was successful
                if isinstance(df, DataFrame):
                    make_report(df, metadata, config)
            else:
                logging.error(f"Failed to load source: `{config['source']}`")
    else:
        logging.warning("Configuration file wasn't found.")


if __name__ == "__main__":
    main()
