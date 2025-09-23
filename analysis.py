import polars as pl
import polars.selectors as cs
from typing import Dict, Any
from utils import logging


def make_analysis(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Perform preliminary analysis using Polars.
    Perform general analysis on the dataframe by days.
    Perform analysis of categorical columns by days:
        - count of uniq values
        - ratio of null values
    Perform analysis of numerical columns by days:
        - max value
        - median value
        - min value
        - average value
        - std_dev of value
    ------------------------------------------------------------------------------
    config: {
        "source": str,  # path to the source file (Excel)
        "date_column": str | None,  # name of the date column (if None, auto-detect)
        "target_column": str | None,  # name of the target column (if any)
        "output": str | None,  # path to the output directory
    }
    ------------------------------------------------------------------------------
    returns: {
        "results": pl.DataFrame,  # dataframe with aggregated results
        "metadata": dict,  # metadata about the analysis
    }
    ------------------------------------------------------------------------------
    """
    df = pl.read_excel(config['source']).lazy()
    schema = df.collect_schema()
    target_column = config.get('target_column')
    date_column = config.get('date_column', get_date_column(schema))
    if not date_column:
        logging.error(
            "There are no date columns"
            "for temporal data distribution analysis.")
        return
    logging.info(f'base date column: {date_column}')

    aggs = [pl.count().alias("__count")]
    if target_column:
        aggs.append(
            pl.col(target_column).mean().alias("__balance")
        )
        metadata = {"__balance": True}
    else:
        metadata = {}
    for col in schema.names():
        if col in (date_column, target_column):
            continue
        aggs.extend([
            pl.col(col).n_unique().alias(f"{col}_uniq"),
            pl.col(col).is_null().mean().alias(f"{col}_null_ratio"),
        ])
        metadata[col] = {"common": ("uniq", "null_ratio")}
        if col in cs.expand_selector(schema, cs.numeric()):
            aggs.extend([
                pl.col(col).min().alias(f"{col}_min"),
                pl.col(col).max().alias(f"{col}_max"),
                pl.col(col).mean().alias(f"{col}_mean"),
                pl.col(col).median().alias(f"{col}_median"),
                pl.col(col).std().alias(f"{col}_std"),
            ])
            metadata[col]["numeric"] = ("min", "max", "mean", "median", "std")
    # Perform aggregations
    output = (
        df.group_by(pl.col(date_column).dt.date().alias('__date'))
        .agg(aggs)
        .sort('__date')
    ).collect()
    return output, metadata


def get_date_column(schema: pl.LazyFrame.schema) -> str:
    """Get the name of the date column in the dataframe."""
    candidates = cs.expand_selector(
        schema,
        cs.date() | cs.datetime()
    )
    if candidates:
        return candidates[0]
