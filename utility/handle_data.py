import polars as pl
from typing import Union, Dict
from types import LambdaType
from .handle_exceptions import exception_handler
from .setup_logging import logging


@exception_handler(exit_on_error=True)
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
        for extension, rff in read_file_func.items():
            if source.endswith(extension):
                return rff(source, storage_options=storage_options).lazy()
        raise ValueError(f"Unrecognized file extension: {source}")

    if isinstance(source, dict):
        if source.get("query") and source.get("uri"):
            return pl.read_database_uri(
                query=source["query"], uri=source["uri"]
            ).lazy()
        elif "file_path" in source:
            storage_options = source.get("storage_options")
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
