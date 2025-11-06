import polars as pl
import polars.selectors as cs
from typing import Dict, Any, Tuple, Union
from utility import logging, exception_handler


@exception_handler(exit_on_error=True)
def make_preprocessing(
        lf: Union[pl.LazyFrame, pl.DataFrame], config: Dict[str, Any]
) -> Tuple[pl.DataFrame, Dict[str, Union[Tuple[str], str]]]:
    """
    Perform data analysis using Polars
    by aggregating column values over dates.
    It includes:
    1. General statistics:
        - number of records per day in dataframe
        - class balance, i.e. average of the target column per day (if present)
    2. Statistics for each column:
        - number of unique values per day
        - ratio of null values per day
    3. Statistic for numerical columns (according to pl.LazyFrame.schema):
        - min value per day
        - max value per day
        - mean value per day
        - median value per day
        - standard deviation per day

    Args:
        config (dict): Configuration dictionary for making analysis and report.

    Returns:
        DataFrame: Aggregated data.
        dict: Metainformation describing aggregated data.
    """
    if isinstance(lf, pl.DataFrame):
        lf = lf.lazy()

    # filter for rows
    filtration = config.get("filtration")
    if filtration and isinstance(filtration, str):
        lf = apply_transformation(lf, filtration, f=True)
    # transformations for columns
    transformation = config.get("transformation")
    if transformation and isinstance(transformation, dict):
        lf = apply_transformation(lf, transformation)

    schema = lf.collect_schema()

    date_column = config.get("date_column", find_date_column(schema))
    if date_column:
        if schema.get(date_column) not in (pl.Date, pl.Datetime):
            lf = lf.with_columns(
                pl.col(date_column).str.to_datetime(strict=True)
            )
    else:
        logging.error("No date columns for data preprocessing")
        return (None, None)

    logging.warning(f"base date column: {date_column}")

    metadata = {}
    aggs = [pl.count().alias("__count")]

    target_column = config.get("target_column")
    if not schema.get(target_column):
        target_column = "target_column"
    if schema.get(target_column):
        aggs.append(
            pl.col(target_column).mean().alias("__target")
        )
    else:
        logging.warning("Target column wasn't found")

    for col in schema.names():
        if col == date_column:
            continue
        aggs.extend([
            pl.col(col).n_unique().alias(f"{col}_uniq"),
            pl.col(col).is_null().mean().alias(f"{col}_null_ratio"),
        ])
        metadata[col] = {
            "dtype": str(schema[col]),
            "common": ("_uniq", "_null_ratio")
        }
        if col in cs.expand_selector(schema, cs.numeric()):
            aggs.extend([
                pl.col(col).min().alias(f"{col}_min"),
                pl.col(col).max().alias(f"{col}_max"),
                pl.col(col).mean().alias(f"{col}_mean"),
                pl.col(col).median().alias(f"{col}_median"),
                pl.col(col).std().alias(f"{col}_std"),
            ])
            metadata[col]["numeric"] = (
                "_min", "_max",
                "_mean", "_median",
                "_std")
    # Perform aggregations
    lf_agg = (
        lf.group_by(pl.col(date_column).dt.date().alias("__date"))
        .agg(aggs)
        .sort("__date")
    )
    lf_agg.explain()
    lf_agg = lf_agg.collect()  # possible exception
    return lf_agg, metadata


@exception_handler()
def find_date_column(schema: pl.LazyFrame.schema) -> Union[str, None]:
    """
    Find the first date column in data schema.

    This function iterates through data schema
    and returns the first column of date or datetime type.

    Args:
        schema (polars.LazyFrame.schema): Data schema provided by Polars.

    Returns:
        Union[str, None]: Name of the first date or datetime column found,
            or None if no such column exists.
    """
    for col, dtype in schema.items():
        if dtype in (pl.Date, pl.Datetime):
            return col


@exception_handler()
def apply_transformation(
        lf: pl.LazyFrame, ft: Union[str, Dict[str, str]], f: bool = False
) -> pl.LazyFrame:
    """
    Apply filters and transformations to a Polars LazyFrame.

    This function applies SQL-based filters and transformations as specified in
    the configuration dictionary. If 'f' is True, a filter applied
    to make a slice of data, otherwise transformation applied
    to alter an existing column.

    Args:
        lf (pl.LazyFrame): Input data as a LazyFrame.
        ft (Union[str, Dict[str, str]]): Filter or transformations.
        f (bool): If True, treat as a filter; otherwise, as a transformation.

    Returns:
        pl.LazyFrame: Filtered or transformed LazyFrame.
    """
    if f and isinstance(ft, str):
        lf = lf.filter(pl.sql_expr(ft))
        logging.info(f"Filter applied: {ft}")
    elif not f and isinstance(ft, dict):
        # Iterate over transformations specified in configuration file
        for alias, expr in ft.items():
            lf = lf.with_columns(pl.sql_expr(expr).alias(alias))
            logging.info(f"Transformation applied: {expr}")
    else:
        logging.warning(f"Unrecognized transformation: {ft}")
    return lf
