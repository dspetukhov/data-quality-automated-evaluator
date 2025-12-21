# Data Quality Automated Evaluation - `DQ-AE`

A configurable Python tool for evaluating the quality of temporal data using **[Polars](https://docs.pola.rs/)** library.

**Why:**

- check and identify data source disruptions followed by performance degradation of a production system,
- to be aware of and ensure governance over data changes over time.

**How:**

- evaluates descriptive statistics (e.g. the number of unique values) for each column in data over specified time intervals,
- composes evaluation results into a structured markdown report with charts and tables representing changes of these statistics over time.

<!-- Detailed description of this tool is available on my **[Medium](https://medium.com/@dspetukhov)** -->

<!-- Particular video example of data evaluation and report interpretation available on my **[YouTube](https://www.youtube.com/@dspetukhov)** -->

**NB**: final verdict about data consistency, validity, and overall quality is the responsibility of an individual reviewing the markdown report.

## Table of contents

- [Quick start](#quick-start)
- [Project overview](#project-overview)
  - [Features](#features)
  - [Structure](#structure)
  - [Configuration](#configuration)
- [Datasets](#datasets)
  - [Kaggle](#kaggle)
  - [Hugging Face](#hugging-face)

## Quick start

1. **Specify source of data:**

    Edit `config.json`:

    ```json
    {
        "source": {
            "file_path": "path/to/your/dataset.csv"
        },
        "date_column": "timestamp"
    }
    ```

2. **Run evaluation:**

    ```bash
    python main.py
    ```

[Back to table of contents](#table-of-contents)

## Project overview

### Features

- **Comprehensive quality evaluation**: a full set of descriptive statistics used as quality metrics to evaluate data changes over custom time intervals.
- **Custom time intervals**: (e.g. 1h, 13h, 1d, 6d, 1d1h, etc.) to comprehend data changes over time.
- **Various data sources**: CSV, XLSX, Parquet, and Iceberg file formats supported as well as reading from cloud providers and PostgreSQL databases.
- **Flexible data preprocessing**: data filtering and transformation using SQL expressions supported by Polars.
- **Outliers detection**: evaluation and visual representation of anomalous changes based on IQR or Z-score criteria.
- **Professional markdown reports**: with formatted tables and customized charts embedded.
- **Configuration in one place**: a variety of preprocessing and reporting options specified in a single, human-readable JSON file.

### Structure

| Module                         | Description                                                                                              |
|--------------------------------|----------------------------------------------------------------------------------------------------------|
| `main.py`                      | Entry point: loads configuration, reads, preprocesses, and evaluates data, generates report              |
| `preprocess.py`                | Preprocesses data by applying a filter and transformations, aggregates data by date                      |
| `evaluate.py`                  | Calculates descriptive statistics for aggregated data, detects outliers based on IQR or Z-score criteria |
| `plot.py`                      | Produces charts with outliers highlighted                                                                |
| `report.py`                    | Produces a structured markdown report with tables and charts embedded                                    |
| `style.css`                    | Report and table styling                                                                                 |
| `config.json`                  | Configuration                                                                                            |
| `utility/__init__.py`          | Default Plotly template, utility imports                                                                 |
| `utility/setup_logging.py`     | Logging configuration                                                                                    |
| `utility/handle_data.py`       | Reads data from file, cloud, or database into Polars LazyFrame                                           |
| `utility/handle_exceptions.py` | Decorator to handle exceptions                                                                           |

### Configuration

Data evaluation configuration is defined in a single JSON file ([config.json](config.json)) by the following sections:

| Section              | Description                                                     | Expected keys                                                     |
|----------------------|-----------------------------------------------------------------|-------------------------------------------------------------------|
| `source`             | Configuration to read data (Required)                           | `file_path`, `file_format`, `storage_options`, `schema_overrides` |
| `output`             | Directory to save report and charts (Optional)                  |                                                                   |
| `filter`             | SQL expression to filter data by rows and by columns (Optional) |                                                                   |
| `transformations`    | Dict of SQL expressions to transform data by columns (Optional) |                                                                   |
| `date_column`        | Column to aggregate data by time intervals (Required)           |                                                                   |
| `time_interval`      | Time inteval to aggregate data (Optional)                       |                                                                   |
| `target_column`      | Column to calculate target average (Optional)                   |                                                                   |
| `columns_to_exclude` | List of columns to be excluded from evaluation (Optional)       |                                                                   |
| `outliers`           | Outlier detection settings (Optional)                           | `criterion`, `multiplier`, `threshold`                            |
| `markdown`           | Markdown report settings (Optional)                             | `name`, `css_style`, `float_precision`                            |
| `plotly`             | Plotly styling settings (Optional)                              | `plot`, `outliers`, `layout`, `grid`, `subplots`, `scale_factor`  |

Each of these sections is described below in detail:

#### `source`

This section specifies parameters to read the source of data using Polars:

- `file_path` is the mandatory value defining the path to the file(s) to read.
- `file_format` is required in cases when file to read is missing an extension at the end of the name or when reading from a directory with partitioned files.
- `storage_options` is required for reading from cloud providers. However, if not explicitly specified, Polars will implicitly try to get relevant credentials from environment variables, so explicit specification of the variables is recommended:

```python
storage_options = {
    # definition with `$` sign as a first symbol will be interpreted as an environment variable to be used
    # definition without `$` sign as a first symbol will be interpreted as a string value
    "aws_access_key_id": "$S3_KEY_ID",
    ...
}
```

- `schema_overrides` can be required for csv or xlsx files to alter column data types during schema inference:
  - when a date or datetime column does not match ISO 8601 standard,
  - when a categorical string column is inferred as a numerical one.

Supported types for `schema_overrides` are `String`, `Date` and `Datetime`.

In case of reading from a PostgreSQL database, all parameters above are replaced by `uri` and `query`:

```python
{
    # `uri` can be specified as an environment variable, e.g. "$PG_URI"
    "uri": "postgresql://username:password@server:port/database",
    "query": "select * from foo"
}
```

#### `output`

This value specifies the directory where the report and charts will be saved. By default, the output directory:

- will be created in the current directory,
- its name will match the file name when reading from a file or `postgresql` when reading from a PostgreSQL database.

It is recommended to define this value.

#### `filter`

This value specifies a SQL expression to filter data by rows and/or by columns using Polars.

#### `transformations`

This section specifies a dict with at least one key-value pair where the key is a column name to be created or replaced and the value is a SQL expression to be applied to one or multiple columns in data. If the key matches any existing column name in data, it replaces that column with transformed values, otherwise a new column is created.

#### `date_column`

This value specifies a date or datetime column to use for aggregating data over time intervals. If not specified, the tool will try to use a column named `date_column`, which can be created with `transformations`.

#### `time_interval`

This value is used to divide the date or datetime range in `date_column` into equal intervals. The division is implemented with [polars.Expr.dt.truncate](https://docs.pola.rs/api/python/stable/reference/expressions/api/polars.Expr.dt.truncate.html). The default value is "1d", which corresponds to one day, so any other value required must be stated explicitly.

#### `target_column`

This optional value specifies a column in data to calculate target average, which is the class balance in machine learning binary classification problems. The calculation result will be shown in the `Overview` section of the markdown report.

If this value is not stated, a column with the name `target_column` will be used as target column. If there is no `target_column` in the data, it can be created with `transformations`.

#### `columns_to_exclude`

This optional value specifies a list of columns to be excluded from the evaluation process.

#### `outliers`

This section specifies parameters to evaluate outliers and highlight outlier areas on charts:

- `criterion` defines a method for outlier detection: `IQR` or `Z-score`. This value is mandatory if outlier areas are expected to be shown on charts.
- `multiplier_iqr` defines the multiplier for the IQR range to determine outlier boundaries (defaults to 1.5).
- `threshold_z_score` defines the Z-score threshold for identifying outliers (defaults to 3.0).

#### `markdown`

This section specifies parameters related to the markdown report being produced:

- `name` defines the name of the report (defaults to `README.md`).
- `css_style` defines a path to the file with CSS style for tables, e.g. [style.css](style.css), which is optional.
- `float_precision` defines the number of decimal places to format floats in markdown tables (defaults to 4).

#### `plotly`

All these Plotly configuration parameters and styles are optional and can be adjusted to match your preferences:

- `plot` defines style for [plotly.graph_objs.Scatter](https://plotly.com/python-api-reference/generated/plotly.graph_objects.Scatter.html#plotly.graph_objects.Scatter), which renders evaluated descriptive statistics over time intervals.
- `outliers` defines style for Plotly shapes to highlight outliers.
- `grid` defines style for grid lines.
- `layout` defines extra parameters to adjust [layout](https://plotly.com/python/reference/layout/). The default chart height equals 512 pixels, default template is `plotly_white`.
- `subplots` define extra parameters to adjust spacing in the [subplot grid](https://plotly.com/python-api-reference/generated/plotly.subplots.make_subplots.html).
- `scale_factor` defines factor to scale a chart, defaults to 1.

[Back to table of contents](#table-of-contents)

## Datasets

Below you can find the list of the publicly available datasets tested in the following Python environment:

```bash
# Python 3.10.12
polars==1.35.2
plotly==6.3.0
```

### [Kaggle](https://www.kaggle.com/datasets?search=fraud&sort=votes&tags=13302-Classification&minUsabilityRating=9.00+or+higher)

- [Metaverse Financial Transactions Dataset](https://www.kaggle.com/datasets/faizaniftikharjanjua/metaverse-financial-transactions-dataset)

```json
    "source": {
        "file_path": "metaverse_transactions_dataset.csv",
        "schema_overrides": {
            "timestamp": "Datetime"
        }
    },
    "date_column": "timestamp",
    "transformations": {
        "target_column": "(anomaly = 'high_risk')::int"
    },
```

- [Credit Card Fraud Prediction](https://www.kaggle.com/datasets/kelvinkelue/credit-card-fraud-prediction)

```json
    "source": {
        "file_path": "fraud test.csv",
        "schema_overrides": {
            "trans_date_trans_time": "Datetime"
        }
    },
    "date_column": "trans_date_trans_time",
    "target_column": "is_fraud",
    "columns_to_exclude": [""],
```

- [Is this a bad transaction?](https://www.kaggle.com/datasets/podsyp/fraud-transactions-detection)

```json
    "source": {
        "file_path": "fraud.csv"
    },
    "date_column": "rep_loan_date",
    "target_column": "bad_flag",
```

- [Ecommerce Counterfeit Products Dataset](https://www.kaggle.com/datasets/aimlveera/counterfeit-product-detection-dataset)

```json
    "source": {
        "file_path": "_counterfeit_transactions.csv"
    },
    "date_column": "transaction_date",
    "target_column": "involves_counterfeit",
    "transformations": {
        "transaction_date": "DATE(transaction_date, '%Y-%m-%d %H:%M:%S')",
        "involves_counterfeit": "involves_counterfeit::int"
    },
```

It is also possible to replace `"transaction_date": "DATE(transaction_date, '%Y-%m-%d %H:%M:%S')",` with:

```json
    "source": {
        "file_path": "_counterfeit_transactions.csv",
        "schema_overrides": {
            "transaction_date": "Datetime"
        }
    },
```

- [Financial Transactions Dataset for Fraud Detection](https://www.kaggle.com/datasets/aryan208/financial-transactions-dataset-for-fraud-detection)

```json
    "source": {
        "file_path": "financial_fraud_detection_dataset.csv",
        "schema_overrides": {
            "timestamp": "Datetime"
        }
    },
    "filter": "select * from self where timestamp::date > '2023-01-01' and timestamp::date < '2024-01-01'",
    "date_column": "timestamp",
    "target_column": "is_fraud",
```

[Back to table of contents](#table-of-contents)

### [Hugging Face](https://huggingface.co/datasets?size_categories=or:%28size_categories:10K%3Cn%3C100K,size_categories:100K%3Cn%3C1M,size_categories:1M%3Cn%3C10M,size_categories:10M%3Cn%3C100M,size_categories:100M%3Cn%3C1B,size_categories:1B%3Cn%3C10B,size_categories:10B%3Cn%3C100B,size_categories:100B%3Cn%3C1T,size_categories:n%3E1T%29&sort=trending&search=fraud)

- [Tichies/card-fraud](https://huggingface.co/datasets/Tichies/card-fraud)

```json
    "source": {
        "file_path": "hf://datasets/Tichies/card-fraud/saske.csv",
        "schema_overrides": {
            "Transaction_Date": "Datetime"
        }
    },
    "date_column": "Transaction_Date",
    "target_column": "isFraud",
```

- [saifhmb/FraudPaymentData](https://huggingface.co/datasets/saifhmb/FraudPaymentData)

```json
    "source": {
        "file_path": "FraudPaymentData.parquet"
    },
    "date_column": "Time_step",
    "target_column": "Label",
    "transformations": {
        "Time_step": "DATE(Time_step, '%m/%d/%Y %H:%M')"
    },
```

**Note:** downloaded by `ds = load_dataset("saifhmb/FraudPaymentData")`, then saved `ds["train"].to_parquet("FraudPaymentData.parquet")`

- [Ransaka/fraud_prediction_300K](https://huggingface.co/datasets/Ransaka/fraud_prediction_300K)

```json
    "source": {
        "file_path": "hf://datasets/Ransaka/fraud_prediction_300K/data_50K.parquet"
    },
    "date_column": "S_2",
    "target_column": "target",
    "columns_to_exclude": ["customer_ID"],
```

- [Phoenix21/mock_fraud-detection-dataset](https://huggingface.co/datasets/Phoenix21/mock_fraud-detection-dataset)

```json
    "source": {
        "file_path": "hf://datasets/Phoenix21/mock_fraud-detection-dataset/transactions.csv",
        "schema_overrides": {
            "timestamp": "Datetime"
        }
    },
    "date_column": "timestamp",
    "target_column": "is_fraud",
    "columns_to_exclude": ["transaction_id"],
```

- [Nooha/cc_fraud_detection_dataset](https://huggingface.co/datasets/Nooha/cc_fraud_detection_dataset)

```json
    "source": {
        "file_path": "cc_fraud_detection_dataset.parquet"
    },
    "date_column": "trans_date",
    "target_column": "is_fraud",
    "columns_to_exclude": ["trans_time", "unix_time"],
```

**Note:** downloaded by `ds = load_dataset("Nooha/cc_fraud_detection_dataset")`, then saved `ds["train"].to_parquet("cc_fraud_detection_dataset.parquet")`

[Back to table of contents](#table-of-contents)
