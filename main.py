import os
import json
from utils import logging
from analysis import make as make_analysis
from report import make as make_report


if __name__ == "__main__":
    if os.path.exists("config.json"):
        with open("config.json") as file:
            try:
                config = json.load(file)
            except json.JSONDecodeError as e:
                logging.error(f"Error parsing configuration file: {e}")
                conifg = None
        if config and os.path.exists(config.get("source")):
            df, metadata = make_analysis(config)
            make_report(df, metadata, config)
    else:
        logging.warning("Configuration file wasn't found.")
