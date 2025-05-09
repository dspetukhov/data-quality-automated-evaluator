# Configuration
config = {
    "date_column": "date",
    "target_column": "target",
    "numerical_columns": ["feature1", "feature2"],
    "categorical_columns": ["category1", "category2"]
}

# Run analysis
results = run_eda("your_data.csv", config)

# Access results
general_stats = results["results"]["general"]
categorical_analysis = results["results"]["categorical"]
numerical_analysis = results["results"]["numerical"]

# Display plots
plt.show()
