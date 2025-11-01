import os
import json
from polars import LazyFrame, DataFrame
from utils import logging
from analysis import make_analysis
from report import make_report
from analysis import read_source


def main():
    """
    Main function to execute the data quality evaluation pipeline.
    Requires `config.json` correct preset.

    This pipeline inclues the following steps:
      1. Checks for the existence of the configuration file.
      2. Loads and parses the configuration from JSON.
      3. Reads the data source as specified in the configuration.
      4. Performs data analysis on the loaded data.
      5. Generates a report based on the analysis results.

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
                # Perform analysis on the data
                df, metadata = make_analysis(source_data, config)
                if isinstance(df, DataFrame):
                    make_report(df, metadata, config)
            else:
                logging.error(f"Failed to load source: `{config['source']}`")
    else:
        logging.warning("Configuration file wasn't found.")


if __name__ == "__main__":
    main()
