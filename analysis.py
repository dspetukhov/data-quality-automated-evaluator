import sys
import polars as pl
import polars.selectors as cs
from typing import Dict, Any, Tuple, Union, Callable
from types import LambdaType
from functools import wraps
from utils import logging
import traceback


def exception_handler(exit_on_error: bool = False):
    """Decorator to handle exceptions."""
    def decorator(func: Callable) -> Callable:
        def make_message(exc_info) -> str:
            exc_type, exc_obj, tb = exc_info
            tb = traceback.extract_tb(tb)[1]
            return "{0}: {1}#{2}: {3}: {4}".format(
                exc_type.__name__,
                tb.filename, tb.lineno,
                tb.line,
                str(exc_obj).lower())

        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception:
                print()
                logging.error(make_message(sys.exc_info()))
            if exit_on_error:
                sys.exit(1)
            return args[0] if args else None
        return wrapper
    return decorator


@exception_handler(exit_on_error=True)
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
    if not isinstance(lf, pl.LazyFrame):
        logging.error(f"Failed to load source: `{config['source']}`")
        return (None, None)

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
            lf = lf.with_columns(  # possible exception
                pl.col(date_column).str.to_datetime(strict=True)
            )
    else:
        date_column = find_date_column(schema)

    if not date_column:
        logging.error("There are no date columns for data analysis")
        return (None, None)

    logging.warning(f"base date column: {date_column}")

    metadata = {}
    aggs = [pl.count().alias("__count")]

    target_column = config.get("target_column")
    if not schema.get(target_column):
        target_column = "target_column"
    if schema.get(target_column):
        n_unique = lf.select(pl.col(target_column).n_unique()).collect().item()
        if n_unique == 2:
            aggs.append(
                pl.col(target_column).mean().alias("__balance")
            )
        else:
            logging.warning(f"Target column `{target_column}` is not binary")
            target_column = None
    else:
        logging.warning("Target column wasn't found")

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
    ).collect()  # possible exception
    return output, metadata


def find_date_column(schema: pl.LazyFrame.schema) -> Union[str, None]:
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
        LazyFrame: The loaded dataframe or None if load was unsuccessful.
    """
    read_file_func = {
        "csv": pl.scan_csv,
        "parquet": pl.scan_parquet,
        "iceberg": pl.scan_iceberg,
        "xlsx": pl.read_excel
    }

    def iterate_extensions(
            source: str,
            storage_options: dict = None
    ) -> pl.LazyFrame:
        """Choose appropriate extension to read the source if possible
        or iterate over possible file extensions.
        """
        extension = source.split(".")[-1].lower()
        if extension in read_file_func:
            return read_file_func[extension](
                source,
                storage_options=storage_options).lazy()
        else:
            for rff in read_file_func.values():
                lf = rff(source, storage_options=storage_options).lazy()
                if isinstance(lf, pl.LazyFrame):
                    return lf
        logging.error("Unsupported file format in source.")

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
                    return rff(
                        source["file_path"],
                        storage_options=storage_options).lazy()
            else:
                return iterate_extensions(source["file_path"], storage_options)
        else:
            logging.error("Incorrect source specification.")
    elif isinstance(source, str):
        return iterate_extensions(source)
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
    @exception_handler()
    def apply(lf: pl.LazyFrame, alias: str, ttype: str, texpr: str, f=False):
        """Apply single transformation."""
        if ttype == "sql":
            lf = lf.sql(texpr) if f else lf.sql("""
                select *, {0} as {1} from self
            """.format(texpr, alias))
        elif ttype == "polars":
            if not texpr.startswith("pl.col"):
                raise ValueError(
                    "Polars expression should start with pl.col()")
            expr = eval(texpr, {"pl": pl})
            if isinstance(expr, pl.Expr):
                lf = lf.filter(expr) if f else lf.with_columns(expr.alias(alias))
            else:
                logging.warning(
                    f"Invalid Polars expression: {texpr}")
        else:
            logging.warning(
                f"Unrecognized type of transformation: `{ttype}`")

        logging.info(f"Transformation applied: {texpr}")
        return lf

    for item in config:
        t = config[item]
        if isinstance(t, str):
            lf = apply(lf, alias=None, ttype=item, texpr=t, f=f)
        if isinstance(t, dict):
            for key, value in t.items():
                lf = apply(lf, item, key, value, f)

    return lf
