import os
import json
from utils import logging
from analysis import make_analysis
from report import make_report


if __name__ == "__main__":
    if os.path.exists("config.json"):
        with open("config.json") as file:
            try:
                config = json.load(file)
            except json.JSONDecodeError as e:
                logging.error(f"Error loading configuration file: {e}")
                conifg = None

        if config and config.get("source"):
            df, metadata = make_analysis(config)
            if df is not None:
                make_report(df, metadata, config)
    else:
        logging.warning("Configuration file wasn't found.")
