# Data quality automated evaluation [`DQ-AE`]

**Description:** tool for data quality (`DQ`) automated evaluation using Polars library. Purposefully designed for sequential data to ensure and assurace data consistency over time for machine learning projects.

## Datasets

### [Kaggle](https://www.kaggle.com/datasets?search=fraud&sort=votes&tags=13302-Classification&minUsabilityRating=9.00+or+higher)

- [Metaverse Financial Transactions Dataset](https://www.kaggle.com/datasets/faizaniftikharjanjua/metaverse-financial-transactions-dataset)

```json
    "source": "metaverse_transactions_dataset.csv",
    "date_column": "timestamp",
    "transformations": {
        "target_column": "(anomaly = 'high_risk')::int"
    },
```

- [IBM Transactions for Anti Money Laundering (AML)](https://www.kaggle.com/datasets/ealtman2019/ibm-transactions-for-anti-money-laundering-aml)

- [Online Retail Transaction Data](https://www.kaggle.com/datasets/thedevastator/online-retail-transaction-data)

```json
    "source": {
        "file_path": "online_retail.csv",
        "schema_overrides": {
            "InvoiceNo": "String"
        }
    },
    "date_column": "InvoiceDate",
    "transformations": {
        "InvoiceDate": "DATE(InvoiceDate, '%m/%d/%Y %H:%M')"
    },
```

- [Credit Card Fraud Prediction](https://www.kaggle.com/datasets/kelvinkelue/credit-card-fraud-prediction)

```json
    "source": {
        "file_path": "../datasets/fraud test.csv"
    },
    "date_column": "trans_date_trans_time",
    "target_column": "is_fraud",
    "transformations": {
        "trans_date_trans_time": "DATE(trans_date_trans_time, '%d/%m/%Y %H:%M')"
    },
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
        "file_path": "../datasets/_counterfeit_transactions.csv"
    },
    "date_column": "transaction_date",
    "target_column": "involves_counterfeit",
    "transformations": {
        "transaction_date": "DATE(transaction_date, '%Y-%m-%d %H:%M:%s')",
        "involves_counterfeit": "involves_counterfeit::int"
    },
```

**TO DO:** add columns to exclude, columns to select in configuration
