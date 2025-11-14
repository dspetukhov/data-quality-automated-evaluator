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
    }
```

- [IBM Transactions for Anti Money Laundering (AML)](https://www.kaggle.com/datasets/ealtman2019/ibm-transactions-for-anti-money-laundering-aml)

- [Online Retail Transaction Data](https://www.kaggle.com/datasets/thedevastator/online-retail-transaction-data)



- [Credit Card Fraud Prediction](https://www.kaggle.com/datasets/kelvinkelue/credit-card-fraud-prediction)

- [Is this a bad transaction?](https://www.kaggle.com/datasets/podsyp/fraud-transactions-detection)

- [Ecommerce Counterfeit Products Dataset](https://www.kaggle.com/datasets/aimlveera/counterfeit-product-detection-dataset)
