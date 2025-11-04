import polars as pl
from typing import Union, Dict
from .handle_exceptions import exception_handler


@exception_handler(exit_on_error=True)
def read_source(source: Union[str, Dict[str, str]]) -> pl.LazyFrame:
    """
    Read specified source of data into Polars LazyFrame.

    Supports CSV, Parquet, Iceberg, XLSX, and database URIs.
    Returns LazyFrame for preprocessing. If loading fails,
    it terminates the main program.

    Args:
        source (Union[str, Dict[str, str]]): Data source specification.
            Can be a string, a dictionary with `query` and `uri` keys
            to read from a PostgreSQL database,
            a dictionary with `file_path` key
            to read from hard drive / Google Drive / S3
            (`storage_options` and `extension` are optional).

    Returns:
        LazyFrame: Polars lazy DataFrame.

    Raises:
        SystemExit: If data cannot be loaded.
    """
    # {file extension: read function} mapping
    read_source_func = {
        "csv": pl.scan_csv,
        "parquet": pl.scan_parquet,
        "iceberg": pl.scan_iceberg,
        "xlsx": pl.read_excel
    }

    def get_read_source_func(
            source: str,
            storage_options: dict = None
    ) -> pl.LazyFrame:
        """
        Get function to read the source based on its ending.
        """
        for extension, read in read_source_func.items():
            if source.endswith(extension):
                return read(source, storage_options=storage_options).lazy()
        # Raise exception if read function wasn't found
        raise ValueError(f"Unrecognized source: {source}")

    if isinstance(source, dict):
        if source.get("query") and source.get("uri"):
            return pl.read_database_uri(
                query=source["query"], uri=source["uri"]
            ).lazy()
        elif "file_path" in source:
            storage_options = source.get("storage_options")
            # Get read function based on extension
            read = read_source_func.get(source.get("extension"))
            # If extension wasn't provided or wasn't found in read_source_func
            if read is None:
                return get_read_source_func(
                    source["file_path"], storage_options)
            else:
                return read(
                    source["file_path"],
                    storage_options=storage_options).lazy()
        else:
            raise ValueError(f"Incorrect source specification: {source}")
    elif isinstance(source, str):
        return get_read_source_func(source)
