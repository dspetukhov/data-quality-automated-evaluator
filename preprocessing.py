import polars as pl
import polars.selectors as cs
from typing import Dict, Any, Tuple, Union
from utility import logging, exception_handler


@exception_handler(exit_on_error=True)
def make_preprocessing(
        lf: Union[pl.LazyFrame, pl.DataFrame], config: Dict[str, Any]
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
    if isinstance(lf, pl.DataFrame):
        lf = lf.lazy()

    # filter for rows
    filtration = config.get("filtration")
    if filtration and isinstance(filtration, dict):
        lf = apply_transformation(lf, filtration, f=True)
    # transformations for columns
    transformation = config.get("transformation")
    if transformation and isinstance(transformation, dict):
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
    output = (
        lf.group_by(pl.col(date_column).dt.date().alias("__date"))
        .agg(aggs)
        .sort("__date")
    )
    output.explain()
    output = output.collect()  # possible exception
    return output, metadata


@exception_handler()
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


@exception_handler()
def apply_transformation(
        lf: pl.LazyFrame, config: dict, f: bool = False
) -> pl.LazyFrame:
    """
    Apply transformations from config.json to the LazyFrame.
    Transformations can be defined as SQL quieries executed by Polars.
    """
    def apply(lf: pl.LazyFrame, alias: str, ttype: str, texpr: str, f=False):
        """Apply single transformation."""
        if ttype == "sql":
            if f:
                if texpr.startswith("select "):
                    lf = lf.sql(texpr)
                else:
                    lf = lf.sql(f"select * from self where {texpr}")
            else:
                lf = lf.sql(f"select *, {texpr} as {alias} from self")
        elif ttype == "polars":
            raise ValueError("Not implemented yet")
        else:
            logging.warning(
                f"Unrecognized type of transformation: `{ttype}`")

        logging.info(f"Transformation applied: {texpr}")
        return lf

    for name, t in config.items():
        if isinstance(t, str):
            lf = apply(lf, alias=None, ttype=name, texpr=t, f=f)
        if isinstance(t, dict):
            for key, value in t.items():
                lf = apply(lf, name, key, value, f)

    return lf
