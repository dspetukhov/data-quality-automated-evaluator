import polars as pl
import polars.selectors as cs
from typing import Dict, Any, Tuple, Union
from utility import logging, exception_handler


@exception_handler(exit_on_error=True)
def make_preprocessing(
        lf: Union[pl.LazyFrame, pl.DataFrame], config: Dict[str, Any]
) -> Tuple[pl.DataFrame, Dict[str, Union[Tuple[str], str]]]:
    """
    Preprocess data for evaluation through per-column aggregation by dates.

    This function processes input data frame by applying filtration
    and transformation as specified by SQL expressions in the configuration.
    It then validates date_column to ensure data aggregation over dates,
    collects aggregation steps and constructs metadata for each column,
    performs aggregation and returns its result with metadata.

    Args:
        lf (Union[pl.LazyFrame, pl.DataFrame]): Input data frame.
        config (Dict[str, Any]): Configuration dictionary specifying
            extra variables, filtration, and transformation parameters.

    Returns:
        Tuple[pl.DataFrame, Dict[str, Union[str, Tuple[str]]]]:
            - Aggregated data with descriptive statistics for each column.
            - Metadata dictionary describing aggregated columns.

    Raises:
        SystemExit: In case of failed data aggregation
            or inconsistent data in date_column.
    """
    if isinstance(lf, pl.DataFrame):
        lf = lf.lazy()

    if "filtration" in config:
        # Apply filter for rows
        lf = apply_transformation(lf, config["filtration"], f=True)
    if "transformation" in config:
        # Apply transformation for columns
        lf = apply_transformation(lf, config["transformation"])

    schema = lf.schema
    # Get date_column from configuration or try to find it in data
    date_column = config.get("date_column", find_date_column(schema))
    if date_column:
        # Check date_column type and convert it if necessary
        if schema.get(date_column) not in (pl.Date, pl.Datetime):
            lf = lf.with_columns(
                pl.col(date_column).str.to_datetime(strict=True))
        # Convert date_column to Polars date type
        lf = lf.with_columns(pl.col(date_column).dt.date().alias("__date"))
    else:
        logging.error("No date columns for data preprocessing")
        return (None, None)

    logging.warning(f"base date column: {date_column}")

    # aggregation expressions for general statistics
    aggs = [pl.count().alias("__count")]

    # If target column was specified in configuration,
    # compute its mean (class balance or target average)
    target_column = config.get("target_column")
    if schema.get(target_column):
        aggs.append(
            pl.col(target_column).mean().alias("__target"))
    else:
        logging.warning("Target column wasn't found")

    metadata = {}
    for col in schema.names():
        if col == date_column:
            continue
        metadata_col = {"dtype": str(schema[col]), "common": [], "numeric": []}
        # Add common statistics for the column
        aggs.extend([
            # number of unique values
            pl.col(col).n_unique().alias(f"{col}_uniq"),
            # ratio of null values
            pl.col(col).is_null().mean().alias(f"{col}_null_ratio"),
        ])
        metadata_col["common"].extend(["_uniq", "_null_ratio"])

        # Add extra statistics if column is of numeric data type
        if col in cs.expand_selector(schema, cs.numeric()):
            aggs.extend([
                pl.col(col).min().alias(f"{col}_min"),
                pl.col(col).max().alias(f"{col}_max"),
                pl.col(col).mean().alias(f"{col}_mean"),
                pl.col(col).median().alias(f"{col}_median"),
                pl.col(col).std().alias(f"{col}_std"),
            ])
            metadata_col["numeric"].extend(
                ["_min", "_max", "_mean", "_median", "_std"]
            )
        metadata[col] = metadata_col

    # Aggregate data by date column
    lf_agg = lf.group_by("__date").agg(aggs).sort("__date")
    lf_agg.explain()
    lf_agg = lf_agg.collect()
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
