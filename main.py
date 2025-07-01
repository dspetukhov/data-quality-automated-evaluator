from eda import make as make_analysis
from report import make as make_report

# Configuration
config = {
    # "date_column": None,
    # "target_column": "target",
    "numerical_columns": ["feature1", "feature2"],
    "categorical_columns": ["category1", "category2"],
    'data_source': '../datasets/online-retail.xlsx'
}

# Run analysis
results = make_analysis(config)
make_report(results)
# Access results
# general_stats = results["results"]["general"]
# categorical_analysis = results["results"]["categorical"]
# numerical_analysis = results["results"]["numerical"]



# Display plots
# plt.show()
