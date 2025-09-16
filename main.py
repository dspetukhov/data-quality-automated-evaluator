import os
import json
from utils import logging
from eda import make as make_analysis
from report import make as make_report

# Configuration
# config = {
    # "date_column": None,
    # "target_column": "target",
    # "numerical_columns": ["feature1", "feature2"],
    # "categorical_columns": ["category1", "category2"],
    # 'data_source': '../datasets/online-retail.xlsx'
# }

# Run analysis
if __name__ == "__main__":
    if os.path.exists("config.json"):
        with open("config.json") as file:
            config = json.load(file)
        df, metadata = make_analysis(config)
        make_report(df, metadata, config)
    else:
        logging.warning("Config file wasn't found.")


# results = make_analysis(config)
# make_report(results)
# Access results
# general_stats = results["results"]["general"]
# categorical_analysis = results["results"]["categorical"]
# numerical_analysis = results["results"]["numerical"]



# Display plots
# plt.show()
