import sys
import polars as pl
import polars.selectors as cs
from typing import Dict, Any, Tuple, Union
from utils import logging


def make_analysis(
        config: Dict[str, Any]
) -> Tuple[pl.LazyFrame, Dict[str, Union[Tuple[str], str]]]:
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
        LazyFrame: Dataframe with aggregated data.
        dict: Metainfo about aggregated data.
    """
    lf = read_source(config['source'])

    # filter for DataFrame rows
    filtration = config.get("filtration")
    if isinstance(filtration, dict):
        lf = apply_transformation(lf, filtration)
    # transformations for DataFrame columns
    transformation = config.get("transformation")
    if isinstance(transformation, dict):
        lf = apply_transformation(lf, transformation)

    schema = lf.collect_schema()

    date_column = config.get("date_column")
    if isinstance(date_column, str):
        if schema.get(date_column) not in (pl.Date, pl.Datetime):
            try:
                lf = lf.with_columns(
                    pl.col(date_column).str.to_datetime(strict=True)
                )
            except Exception as e:
                logging.error(
                    f"Failed to convert '{date_column}' to date: {e}")
                sys.exit(1)
    else:
        date_column = get_date_column(schema)

    if not date_column:
        logging.error(
            "There are no date columns "
            "for temporal data distribution analysis.")
        return (None, None)

    logging.warning(f'base date column: {date_column}')

    metadata = {}
    aggs, = [pl.count().alias("__count")]

    target_column = config.get("target_column") \
        if "target_column" in config \
        else schema.get("target_column")

    if not target_column:
        logging.warning("Target column wasn't specified")
    else:
        n_unique = lf.select(pl.col(target_column).n_unique()).collect().item()
        if n_unique == 2:
            aggs.append(
                pl.col(target_column).mean().alias("__balance")
            )
            metadata = {"__balance": "Class balance"}
        else:
            logging.warning(f"Target column `{target_column}` is not binary")

    for col in schema.names():
        if col in (date_column, target_column):
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
    output = (
        lf.group_by(pl.col(date_column).dt.date().alias('__date'))
        .agg(aggs)
        .sort('__date')
    ).collect()
    # print(output, metadata) "valuation": ("_min", "_max", "_cardinality")
    return output, metadata


def get_date_column(schema: pl.LazyFrame.schema) -> Union[str, None]:
    """
    Get the name of the date column in the dataframe.

    Args:
        schema (polars.LazyFrame.schema): The schema of the dataframe.

    Returns:
        str: The name of the first date column, or None if not found.
    """
    candidates = cs.expand_selector(
        schema,
        cs.date() | cs.datetime()
    )
    if candidates:
        if "date_column" in candidates:
            return "date_column"
        return candidates[0]


def read_source(source: str) -> pl.LazyFrame:
    """
    Read the source file into a Polars LazyFrame.

    Args:
        source (str): Path to the source file.

    Returns:
        LazyFrame: The loaded dataframe.
    """
    if source.endswith(".csv"):
        return pl.scan_csv(source)
    elif source.endswith(".parquet"):
        return pl.scan_parquet(source)
    elif source.endswith(".iceberg"):
        return pl.scan_iceberg(source)
    elif source.endswith(".xlsx"):
        return pl.read_excel(source).lazy()
    # elif isinstance(source, dict):
        # if all([k in ("query", "uri") for k in source.keys()]):
            # lf = pl.read_database_uri(query=source["query"], uri=source["uri"])
    elif source.startswith("s3://"):
        lf = pl.scan_csv(source)
        if not lf:
            lf = pl.scan_parquet(source)
        if not lf:
            lf = pl.scan_iceberg(source)
        if not lf:
            logging.error(f"No supported file formats found in `{source}`")
            return None
    else:
        logging.error(f"Unsupported file format: `{source}`")
        return None


def apply_transformation(
        lf: pl.LazyFrame, config: dict
) -> pl.LazyFrame:
    """
    Apply transformations from config.json to the dataframe.
    - Supports Polars expressions, lambda functions, and SQL-like expressions.
    - Exits on date column failure, logs and continues on target column failure.
    """
    for item in config:
        t = config[item]
        print(item, t)
        if isinstance(t, str):
            if item == "sql":
                lf = lf.sql(t)
                logging.info(f"The following transformation applied: {t}")
            elif item == "polars":
                pass
            else:
                logging.warning("Unrecognized type of transformation: {item}")
        elif isinstance(t, dict):
            if t.get("sql"):
                lf = lf.sql("""
                    select *, {0} as {1} from self
                """.format(t["sql"], item))
            elif config[item].get("polars"):
                pass
            else:
                logging.warning("Unrecognized type of transformation")
            logging.info(f"The following transformation applied: {t}")
        else:
            logging.warning(
                f"Unrecognized type of transformation for `{item}`: {type(config[item])}"
                )
    return lf
