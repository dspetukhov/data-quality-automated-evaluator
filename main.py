import os
import json
from polars import LazyFrame
from utils import logging
from analysis import make_analysis
from report import make_report
from analysis import read_source


if __name__ == "__main__":
    if os.path.exists("config.json"):
        with open("config.json") as file:
            try:
                config = json.load(file)
            except json.JSONDecodeError as e:
                logging.error(f"Error loading configuration file: {e}")
                conifg = None

        if config and config.get("source"):
            lf = read_source(config["source"])
            if not isinstance(lf, LazyFrame):
                logging.error(f"Failed to load source: `{config['source']}`")
            else:
                df, metadata = make_analysis(lf, config)
                if df is not None:
                    make_report(df, metadata, config)
    else:
        logging.warning("Configuration file wasn't found.")
