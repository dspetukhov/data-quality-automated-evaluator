import polars as pl
import polars.selectors as cs
from typing import Dict, Any, List, Tuple, Union
from utility import logging, exception_handler


@exception_handler(exit_on_error=True)
def make_preprocessing(
        lf: Union[pl.LazyFrame, pl.DataFrame], config: Dict[str, Any]
) -> Tuple[pl.DataFrame, Dict[str, Dict[str, str]]]:
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
        Tuple[pl.DataFrame, Dict[str, Dict[str, str]]:
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
    # Apply transformation for columns
    lf = apply_transformations(lf, config.get("transformations"))

    # Get LazyFrame schema
    schema = lf.collect_schema()

    # Get date_column
    date_column = get_date_column(config, schema)
    # Validate date_column
    lf, schema = validate_date_column(lf, schema, date_column)

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

    # Aggregate data by dates
    lf_agg = lf.group_by("__date").agg(aggs).sort("__date")
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
        lf: pl.LazyFrame, transformations: List[Dict[str, str]]
) -> pl.LazyFrame:
    """
    Apply transformations to Polars LazyFrame.

    This function applies SQL transformations to alter LazyFrame columns
    as specified in the configuration.
    It can create a new column or replace an existing one
    if its name will match the key in a single transformation.

    Args:
        lf (pl.LazyFrame): Input data.
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
    elif transformations is not None:
        logging.warning(f"Unrecognized transformations: {transformations}")
    return lf


@exception_handler()
def get_date_column(
    config: Dict[str, Any],
    schema: pl.LazyFrame.schema
) -> Union[str, None]:
    """
    Determine date_column for data aggregation.

    This function looks for date_column in configuration
    taking `date_column` as a default name. If it is not found in schema,
    it gets the first column of date or datetime type from data schema.
    Returns None if nothing is found.

    Args:
        config (Dict[str, Any]): Configuration dictionary.
        schema (pl.LazyFrame.schema): Data schema provided by Polars.

    Returns:
        Union[str, None]: Name of the date column found,
            or None if no such column exists.
    """
    date_column = config.get("date_column", "date_column")
    if schema.get(date_column):
        return date_column
    else:
        for col, dtype in schema.items():
            if dtype in (pl.Date, pl.Datetime):
                return col


@exception_handler(exit_on_error=True)
def validate_date_column(
    lf: pl.LazyFrame,
    schema: pl.LazyFrame.schema,
    date_column: str
) -> pl.LazyFrame:
    """
    Validate date_column type and make conversion if necessary.

    This function checks date_column type in schema, makes conversion
    to date type if necessary, and renames it as `__date`
    to ensure consistency in data evaluation pipeline.

    Args:
        lf (pl.LazyFrame): Input data.
        schema (pl.LazyFrame.schema): Data schema provided by Polars.
        date_column (str): Data schema provided by Polars.

    Returns:
        Tuple[pl.LazyFrame, pl.LazyFrame.schema]:
            - LazyFrame with date column processed.
            - Updated data schema.

    Raises:
        SystemExit: If data cannot be loaded.
    """
    if date_column:
        # Check date_column type and convert it to Polars date type
        if isinstance(schema.get(date_column), pl.String):
            lf = lf.with_columns(
                pl.col(date_column).str.to_date(strict=True))
        elif isinstance(schema.get(date_column), pl.Datetime):
            lf = lf.with_columns(pl.col(date_column).dt.date())

        # Rename date_column as `__date`
        lf = lf.rename({date_column: "__date"})
        logging.info(f"Date column: `{date_column}`")
        return lf, lf.collect_schema()
    else:
        schema_str = "\n".join(
            f"{col}: {dtype}" for col, dtype in schema.items())
        logging.info(f"Data schema:\n{schema_str}")
        raise SystemExit("There is no date_column for data preprocessing")


@exception_handler()
def collect_aggregations(
    schema: pl.LazyFrame.schema,
    target_column: str,
    columns_to_exclude: List[str]
) -> Tuple[List[str], Dict[str, str]]:
    """
    Collect aggregation expressions.

    This function collects expressions to perform data aggregation by dates.
    The first expression calculates the number of values per date,
    the rest are column-wise statistics.
    Numeric columns marked in metadata dictionary by their types.

    Args:
        schema (pl.LazyFrame.schema): Data schema provided by Polars.
        target_column (str): Target column to compute target average.

    Returns:
        Tuple[List[str], Dict[str, str]]:
            - aggs (List[str]): Aggregation expressions.
            - metadata (Dict[str]): Dict of aggregated columns
                indicating types for numeric columns
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
        if col == "__date" or col in columns_to_exclude:
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
