# Data quality automated evaluation [`DQ-AE`]

**Description:** tool for data quality (`DQ`) automated evaluation using Polars library. Purposefully designed for sequential data to ensure and assurace data consistency over time for machine learning projects.

## Datasets

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

```

- []()

```json

```

- []()

```json

```

- []()

```json

```

- []()

```json

```