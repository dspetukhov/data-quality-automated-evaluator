# Data quality automated evaluation [`DQ-AE`]

A configurable Python tool for evaluating the quality of sequential data using Polars library.

**Why:**

- check data consistency and identify data sources disruptions followed by performace degradation of a production system,
- be aware of and ensure governance over data changes over time.

**How it works:**

1. Read source of data as Polars LazyFrame.
2. Identify date column as a base for data aggregation by dates.
3. Collect aggregation expressions and perform aggregation.
4. Evaluate statistics on aggregated data and produce plots.
5. Collect evaluation results to produce structured markdown report.

**NB**: A method of close look is expected to be applied manually to verify data consistency, validity, and overall quality.

## Table of contents

- []()

## Quick Start

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

2. **Run Analysis**

    ```bash
    python main.py
    ```

## Features

- **Comprehensive data quality evaluation**: A robust set of statistics as quality metrics to evaluate data over time.
- **Flexible preprocessing**: Data filtering and transformations using SQL syntax handled by Polars.
- **Outlier analysis**: Evaluation and visualization of anomalous data points based on IQR and Z-score criteria.
- **Pretty visualizations**: Creates customized plots with outliers highlighted using Plotly.
- **Professional markdown reports**: Produces professional markdown report with stylish tables and plots.
- **Configurable via JSON**: Primary preprocessing and reporting options are specified through a single, human-readable configuration file.

## Project overview

| Module                         | Description                                                                       |
|--------------------------------|-----------------------------------------------------------------------------------|
| `main.py`                      | Entry point: loads configuration, reads data, preprocesses, generates report      |
| `preprocess.py`                | Preprocesses data by applying filter and transformations, aggregates data by date |
| `evaluate.py`                  | Computes statistics and detects outliers based on IQR and Z-score criteria        |
| `plot.py`                      | Produces plots with outliers highlighted                                          |
| `report.py`                    | Produces structured markdown report with tables and plots embedded                |
| `config.json`                  | Configuration                                                                     |
| `style.css`                    | Report and table styling                                                          |
| `utility/handle_data.py`       | Reads data from file, cloud, or database into a Polars LazyFrame                  |
| `utility/handle_exceptions.py` | Decorator to handle exceptions                                                    |
| `utility/setup_logging.py`     | Logging configuration                                                             |

The project uses a single JSON configuration file (`config.json`) with the following main sections:

| Section         | Description                        | Key fields                                                        |
|-----------------|------------------------------------|-------------------------------------------------------------------|
| `source`        | Data input configuration           | `file_path`, `file_format`, `storage_options`, `schema_overrides` |
| `date_column`   | Column to aggregate data by dates  |                                                                   |
| `target_column` | Column to calculate target average |                                                                   |
| `outliers`      | Outlier detection settings         | `criterion`, `multiplier`, `threshold`                            |
| `markdown`      | Markdown report settings           | `name`, `css_style`, `float_precision`                            |
| `plotly`        | Plotly styling settings            | `plot`, `outliers`, `layout`, `grid`, `subplots`, `misc`          |

## Datasets

These are the datasets tested:

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
        "file_path": "../datasets/_counterfeit_transactions.csv",
        "schema_overrides": {
            "transaction_date": "Datetime"
        }
    },
```

- [Financial Transactions Dataset for Fraud Detection](https://www.kaggle.com/datasets/aryan208/financial-transactions-dataset-for-fraud-detection)

```json
    "source": {
        "file_path": "../datasets/financial_fraud_detection_dataset.csv",
        "schema_overrides": {
            "timestamp": "Datetime"
        }
    },
    "filter": "select * from self where timestamp::date > '2023-01-01' and timestamp::date < '2024-01-01'",
    "date_column": "timestamp",
    "target_column": "is_fraud",
```

### [HiggingFace](https://huggingface.co/datasets?size_categories=or:%28size_categories:10K%3Cn%3C100K,size_categories:100K%3Cn%3C1M,size_categories:1M%3Cn%3C10M,size_categories:10M%3Cn%3C100M,size_categories:100M%3Cn%3C1B,size_categories:1B%3Cn%3C10B,size_categories:10B%3Cn%3C100B,size_categories:100B%3Cn%3C1T,size_categories:n%3E1T%29&sort=trending&search=fraud)

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
        "file_path": "../datasets/cc_fraud_detection_dataset.parquet"
    },
    "date_column": "trans_date",
    "target_column": "is_fraud",
    "columns_to_exclude": ["trans_time", "unix_time"],
```

**Note:** downloaded by `ds = load_dataset("Nooha/cc_fraud_detection_dataset")`, then saved `ds["train"].to_parquet("cc_fraud_detection_dataset.parquet")`
