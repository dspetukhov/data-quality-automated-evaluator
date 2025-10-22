import sys
import polars as pl
import polars.selectors as cs
from typing import Dict, Any, Tuple, Union
from types import LambdaType
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
    lf = read_source(config["source"])

    # filter for DataFrame rows
    filtration = config.get("filtration")
    if isinstance(filtration, dict):
        lf = apply_transformation(lf, filtration, f=True)
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

    logging.warning(f"base date column: {date_column}")

    metadata = {}
    aggs = [pl.count().alias("__count")]

    target_column = config.get("target_column")
    if not target_column:
        if schema.get("target_column"):
            target_column = "target_column"
        else:
            logging.warning("Target column wasn't specified")

    n_unique = lf.select(pl.col(target_column).n_unique()).collect().item()
    if n_unique == 2:
        aggs.append(
            pl.col(target_column).mean().alias("__balance")
        )
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
        lf.group_by(pl.col(date_column).dt.date().alias("__date"))
        .agg(aggs)
        .sort("__date")
    ).collect()
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


def read_source(source: Union[str, Dict[str, str]]) -> pl.LazyFrame:
    """
    Read the source file into a Polars LazyFrame.

    Args:
        source (str, dict): Data source specification.

    Returns:
        LazyFrame: The loaded dataframe.
    """
    read_file_func = {
        "csv": pl.scan_csv,
        "parquet": pl.scan_parquet,
        "iceberg": pl.scan_iceberg,
        "xlsx": pl.read_excel
    }
    if isinstance(source, dict):
        if "query" in source and "uri" in source:
            return pl.read_database_uri(
                query=source["query"], uri=source["uri"]
            ).lazy()
        elif "file_path" in source:
            storage_options = source.get("storage_options", None)
            if "extension" in source:
                rff = read_file_func.get(source["extension"])
                if isinstance(rff, LambdaType):
                    return rff(source["file_path"]).lazy()
            else:
                for rff in read_file_func.values():
                    lf = rff(
                        source["file_path"],
                        storage_options=storage_options
                    ).lazy()
                    if isinstance(lf, pl.LazyFrame):
                        return lf
            logging.error("Unsupported file extension in source.")
            return None
        else:
            logging.error("Incorrect source specification.")
            return None
    elif isinstance(source, str):
        if source.endswith(".csv"):
            return read_file_func["csv"](source)
        elif source.endswith(".parquet"):
            return read_file_func["parquet"](source)
        elif source.endswith(".iceberg"):
            return read_file_func["iceberg"](source)
        elif source.endswith(".xlsx"):
            return read_file_func["xlsx"](source).lazy()
        else:
            for rff in read_file_func.values():
                lf = rff(source).lazy()
                if isinstance(lf, pl.LazyFrame):
                    return lf
            logging.error(f"Unsupported source: `{source}`")
            return None
    else:
        logging.error(f"Unrecognized source type: `{type(source)}`")
        return None


def apply_transformation(
        lf: pl.LazyFrame, config: dict, f: bool = False
) -> pl.LazyFrame:
    """
    Apply transformations from config.json to the LazyFrame.
    Transformations can be defined as Polars expressions
    or SQL quieries powered by Polars.
    """
    def apply(lf: pl.LazyFrame, alias: str, ttype: str, texpr: str, f=False):
        """Apply single transformation."""
        try:
            if ttype == "sql":
                lf = lf.sql(texpr) if f else lf.sql("""
                    select *, {0} as {1} from self
                """.format(texpr, alias))
            elif ttype == "polars":
                if not texpr.startswith("pl.col"):
                    raise ValueError(
                        "Expression should start with pl.col()")
                expr = eval(texpr, {"pl": pl})
                if isinstance(expr, pl.Expr):
                    lf = lf.filter(expr) if f else lf.with_columns(expr.alias(alias))
                else:
                    logging.warning(
                        f"Invalid Polars expression: {texpr}")
            else:
                logging.warning(
                    f"Unrecognized type of transformation: `{ttype}`")
        except Exception as e:
            logging.error(
                f"Failed to apply transformation `{texpr}`: {e}")
            return lf

        logging.info(f"The transformation applied: {texpr}")
        return lf

    for item in config:
        t = config[item]
        if isinstance(t, str):
            lf = apply(lf, alias=None, ttype=item, texpr=t, f=f)
        if isinstance(t, dict):
            for key, value in t.items():
                lf = apply(lf, item, key, value, f)

    return lf
