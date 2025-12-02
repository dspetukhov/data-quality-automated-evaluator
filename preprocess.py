import polars as pl
import polars.selectors as cs
from typing import Dict, Any, List, Tuple, Union, Optional
from utility import logging, exception_handler


@exception_handler(exit_on_error=True)
def make_preprocessing(
        lf: Union[pl.LazyFrame, pl.DataFrame], config: Dict[str, Any]
) -> Tuple[pl.DataFrame, Dict[str, str]]:
    """
    Preprocess data for evaluation through aggregation by dates.

    This function processes input data frame by applying filter
    and transformations as specified by SQL expressions in the configuration.
    Then it gets and validates date_column to ensure data aggregation by dates,
    collects aggregation expressions and metadata for each column,
    and performs aggregation.

    Args:
        lf (Union[pl.LazyFrame, pl.DataFrame]): Input data frame.
        config (Dict[str, Any]): Configuration dictionary specifying
            extra variables, filter, and transformations.

    Returns:
        Tuple[pl.DataFrame, Dict[str, str]]:
            - Aggregated data with descriptive statistics for each column.
            - Dictionary of columns indicating types of numeric columns.

    Raises:
        SystemExit: In case of failed data aggregation
            or inconsistencies in date_column.
    """
    # Convert DataFrame to LazyFrame if necessary
    if isinstance(lf, pl.DataFrame):
        lf = lf.lazy()

    # Apply filter for rows and columns
    lf = apply_filter(lf, config.get("filter"))
    # Apply transformations for columns
    lf = apply_transformations(lf, config.get("transformations"))

    # Get LazyFrame schema
    schema = lf.collect_schema()

    # Prepare date_column for data aggregation
    date_column = config.get("date_column", "date_column")
    lf, schema = validate_date_column(
        lf, schema,
        date_column, config.get("time_interval", "1d")
    )

    # Get target_column
    target_column = config.get("target_column", "target_column")
    if not schema.get(target_column):
        target_column = None
        logging.warning("Target column wasn't found")

    # Collect aggregation expressions for each column except excluded ones
    aggs, metadata = collect_aggregations(
        schema,
        target_column,
        config.get("columns_to_exclude", []))

    # Aggregate data by time intervals / buckets
    lf_agg = lf.group_by("__time_interval").agg(aggs).sort("__time_interval")
    # lf_agg.explain()  # uncomment to get the query plan or turn off/on optimizations
    lf_agg = lf_agg.collect()
    return lf_agg, metadata


@exception_handler()
def apply_filter(
        lf: pl.LazyFrame, filter_str: str
) -> pl.LazyFrame:
    """
    Apply filter to Polars LazyFrame.

    This function applies SQL expression to make a slice of data
    as specified in the configuration.

    Args:
        lf (pl.LazyFrame): Input data.
        filter_str (str): SQL expression to filter data.

    Returns:
        pl.LazyFrame: Filtered LazyFrame.
    """
    if isinstance(filter_str, str):
        lf = lf.sql(filter_str)
        logging.info(f"Filter applied: {filter_str}")
    return lf


@exception_handler()
def apply_transformations(
        lf: pl.LazyFrame, transformations: Optional[Dict[str, str]]
) -> pl.LazyFrame:
    """
    Apply transformations to Polars LazyFrame.

    This function applies transformations to alter LazyFrame columns
    as specified by SQL in the configuration.
    If key in transformations matches existing column name,
    it replaces it, otherwise creates a new column.

    Args:
        lf (pl.LazyFrame): Input data.
        transformations (Optional[Dict[str, str]]): Dictionary where:
            each key is column name to be created or replaced,
            each value is SQL expression to transform LazyFrame.

    Returns:
        pl.LazyFrame: LazyFrame with transformed columns.
    """
    if isinstance(transformations, dict):
        # Iterate over transformations specified in configuration file
        for alias, expr in transformations.items():
            lf = lf.with_columns(pl.sql_expr(expr).alias(alias))
            logging.info(f"Transformation applied: {expr}")
    elif transformations is not None:
        logging.warning(f"Unrecognized transformations: {transformations}")
    return lf


@exception_handler(exit_on_error=True)
def validate_date_column(
    lf: pl.LazyFrame,
    schema: Dict[str, pl.DataType],
    date_column: str,
    time_interval: str
) -> Tuple[pl.LazyFrame, Dict[str, pl.DataType]]:
    """
    Check date_column presence in schema and validate its type.

    This function checks date_column type in schema, makes conversion
    to datetime type if necessary, and renames it as `__time_interval`
    to ensure consistency in data evaluation pipeline.
    Division by time intervals implemented with polars.Expr.dt.truncate.
    Raises SystemExit if date_column is not found in schema.

    Args:
        lf (pl.LazyFrame): Input data.
        schema (Dict[str, pl.DataType]): Data schema provided by Polars.
        date_column (str): Name of date column.
        time_interval (str): Time interval to truncate date_column,
            "1d" for one day by default, or "1h" for one hour, etc.

    Returns:
        Tuple[pl.LazyFrame, Dict[str, pl.DataType]]:
            - LazyFrame with date column processed.
            - Updated data schema.

    Raises:
        SystemExit: If data cannot be loaded.
    """
    if schema.get(date_column):
        # Check date_column type and convert it to Polars date type
        if isinstance(schema.get(date_column), pl.String):
            lf = lf.with_columns(
                pl.col(date_column).str.to_date(strict=True))

        # Divide date or datetime range into time intervals / buckets
        lf = lf.with_columns(pl.col(date_column).dt.truncate(time_interval))

        # Rename date_column as `__time_interval` for consistency
        lf = lf.rename({date_column: "__time_interval"})
        logging.info(f"Date column: `{date_column}`")

        return lf, lf.collect_schema()
    else:
        schema_str = "\n".join(
            f"{col}: {dtype}" for col, dtype in schema.items())
        logging.info(f"Data schema:\n{schema_str}")
        raise SystemExit("There is no date_column for data preprocessing")


@exception_handler()
def collect_aggregations(
    schema: Dict[str, pl.DataType],
    target_column: str,
    columns_to_exclude: Optional[List[str]]
) -> Tuple[List[pl.Expr], Dict[str, str]]:
    """
    Collect aggregation expressions.

    This function collects expressions to perform data aggregation by dates.
    The first expression calculates the number of values per date,
    the rest are column-wise statistics.
    Numeric columns marked in metadata dictionary by their types.

    Args:
        schema (Dict[str, pl.DataType]): Data schema provided by Polars.
        target_column (str): Target column to compute target average.
        columns_to_exclude (Optional[List[str]]): List of columns
            to be excluded from aggregation.

    Returns:
        Tuple[List[pl.Expr], Dict[str, str]]:
            - aggs (List[pl.Expr]): Aggregation expressions,
            - metadata (Dict[str, str]): Dict of aggregated columns
                indicating types for numeric columns.
    """
    # Start with common aggregation expression for the number of values
    aggs = [pl.count().alias(" __Number of values")]

    # If target column was found in schema,
    # calculate its mean (i.e. class balance in binary classification problems)
    if target_column:
        aggs.append(
            pl.col(target_column).mean().alias(" __Target average"))

    metadata = {}

    for col in schema.names():
        if col == "__time_interval" or col in columns_to_exclude:
            continue
        # Add common statistics for the column
        aggs.extend([
            pl.col(col).n_unique().alias(
                f"__ {col} __Number of unique values"),
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
