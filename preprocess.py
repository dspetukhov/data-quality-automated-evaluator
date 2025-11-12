import polars as pl
import polars.selectors as cs
from typing import Dict, Any, List, Tuple, Union
from utility import logging, exception_handler


@exception_handler(exit_on_error=True)
def make_preprocessing(
        lf: Union[pl.LazyFrame, pl.DataFrame], config: Dict[str, Any]
) -> Tuple[pl.DataFrame, Dict[str, Dict[str, str]]]:
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
        Tuple[pl.DataFrame, Dict[str, Dict[str, str]]:
            - Aggregated data with descriptive statistics for each column.
            - Dictionary of columns indicating types of numeric columns.

    Raises:
        SystemExit: In case of failed data aggregation
            or inconsistent data in date_column.
    """
    if isinstance(lf, pl.DataFrame):
        lf = lf.lazy()

    lf = apply_filter(lf, config.get("filter"))
    # if "filtration" in config:
    #     # Apply filter for rows
    #     lf = apply_transformation(lf, config["filtration"], f=True)
    # if "transformation" in config:
        # Apply transformation for columns
    lf = apply_transformations(lf, config["transformations"])

    schema = lf.collect_schema()
    # ! rethink date_column preparation 
    # Get date_column from configuration or try to find it in data
    # separate into single function
    # get_date_column()
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

    # Start with common aggregation expression for the number of values
    aggs = [pl.count().alias(" __Number of values")]

    # add a separate function for target_column
    # If target column was specified in configuration,
    # compute its mean (class balance or target average)
    # ! separate into single function
    target_column = config.get("target_column", "target_column")
    if schema.get(target_column):
        aggs.append(
            pl.col(target_column).mean().alias(" __Target average"))
    else:
        logging.warning("Target column wasn't found")

    # Collect aggregation expressions for all columns except date_column
    aggs, metadata = collect_aggregations(aggs, schema)

    # Aggregate data by date column
    lf_agg = lf.group_by("__date").agg(aggs).sort("__date")
    lf_agg.explain()
    lf_agg = lf_agg.collect()
    return lf_agg, metadata


@exception_handler()
def apply_filter(
        lf: pl.LazyFrame, filter_str: str
) -> pl.LazyFrame:
    """
    Apply filter to Polars LazyFrame.

    This function applies SQL filter to make a slice of data
    as specified in the configuration.

    Args:
        lf (pl.LazyFrame): Input data as Polars LazyFrame.
        filter_str (str): SQL expression to filter data.

    Returns:
        pl.LazyFrame: Filtered LazyFrame.
    """
    if isinstance(filter_str, str):
        lf = lf.filter(pl.sql_expr(filter_str))
        logging.info(f"Filter applied: {filter_str}")
    return lf


@exception_handler()
def apply_transformations(
        lf: pl.LazyFrame, transformations: List[Dict[str, str]]
) -> pl.LazyFrame:
    """
    Apply transformations to Polars LazyFrame.

    This function applies SQL transformations to alter LazyFrame columns
    as specified in the configuration.
    It can create a new column or replace an existing one
    if its name will match the key in a single transformation.

    Args:
        lf (pl.LazyFrame): Input data as a LazyFrame.
        transformations (List[Dict[str]]): List of dicts:
            each dict contains column name as a key
            and SQL expression to transform LazyFrame as a value.

    Returns:
        pl.LazyFrame: LazyFrame with transformed columns.
    """
    if isinstance(transformations, dict):
        # Iterate over transformations specified in configuration file
        for alias, expr in transformations.items():
            lf = lf.with_columns(pl.sql_expr(expr).alias(alias))
            logging.info(f"Transformation applied: {expr}")
    else:
        logging.warning(f"Unrecognized transformations: {transformations}")
    return lf


# TO DO: modify
@exception_handler()
def get_date_column(schema: pl.LazyFrame.schema) -> Union[str, None]:
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
def get_target_column(schema: pl.LazyFrame.schema) -> Union[str, None]:
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
def collect_aggregations(aggs: List[str], schema, date) -> Tuple[List[str], Dict[str]]:
    """

    """
    metadata = {}
    for col in schema.names():
        if col == date_column:
            continue
        # Add common statistics for the column
        aggs.extend([
            # Number of unique values
            pl.col(col).n_unique().alias(
                f"__ {col} __Number of unique values"),
            # Ratio of null values
            pl.col(col).is_null().mean().alias(
                f"__ {col} __Ratio of null values"),
        ])

        # Add extra statistics if column is of numeric data type
        if col in cs.expand_selector(schema, cs.numeric()):
            aggs.extend([
                pl.col(col).min().alias(f"n__ {col} __Min"),
                pl.col(col).max().alias(f"n__ {col} __Max"),
                pl.col(col).mean().alias(f"n__ {col} __Mean"),
                pl.col(col).median().alias(f"n__ {col} __Median"),
                pl.col(col).std().alias(f"n__ {col} __Standard deviation"),
            ])
            metadata[col] = str(schema[col])
        else:
            metadata[col] = None

    return aggs, metadata
