import sys
import json
from pathlib import Path
from utility import logging, exception_handler, read_source
from preprocess import make_preprocessing
from report import make_report


@exception_handler()
def main(config_file_path: Path) -> None:
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
        SystemExit: If the configuration file wasn't found
            or doesn't have 'source' section.
    """
    # Try to load the configuration file
    config_file_path = Path(config_file_path)
    if config_file_path.exists() and config_file_path.is_file():
        with open(config_file_path, encoding="utf-8") as file:
            config = json.load(file)
            logging.info(f"Configuration from {config_file_path} was loaded")
    else:
        raise SystemExit("Configuration file wasn't found")

    # Proceed if configuration was loaded and contains `source`
    if config.get("source"):
        source_data = read_source(config["source"])
        # Preprocess data
        df, metadata = make_preprocessing(source_data, config)
        # Generate a report if preprocessing was successful
        if df is not None and metadata is not None:
            make_report(df, metadata, config)
        else:
            raise SystemExit("Preprocessing failed; report won't be created")
    else:
        raise SystemExit("Configuration is missing required 'source' section")


if __name__ == "__main__":
    if len(sys.argv) == 2:
        main(Path(sys.argv[1]))
    else:
        raise SystemExit("Usage: python main.py <config_file_path>")
