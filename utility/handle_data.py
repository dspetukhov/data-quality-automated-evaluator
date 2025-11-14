import polars as pl
from typing import Union, Any, Dict
from .handle_exceptions import exception_handler

# {file extension: read function} mapping
read_source_func = {
    "csv": pl.scan_csv,
    "parquet": pl.scan_parquet,
    "iceberg": pl.scan_iceberg,
    "xlsx": pl.read_excel
}


@exception_handler(exit_on_error=True)
def read_source(source: Union[str, Dict[str, str]]) -> pl.LazyFrame:
    """
    Read specified source of data as Polars LazyFrame.

    Supports CSV, Parquet, Iceberg, XLSX, and database URIs.
    Returns LazyFrame for preprocessing. If loading fails,
    it terminates the main program.
    There is no need in specifying `storage_options` explicitly
    as Polars can read corresponding environmental variables,
    e.g. `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_REGION`.

    Args:
        source (Union[str, Dict[str, str]]): Data source specification.
            Can be a string, a dictionary with `query` and `uri` keys
            to read from a PostgreSQL database,
            a dictionary with `file_path` key
            to read from hard drive / Google Drive / S3
            (`storage_options` and `extension` are optional).

    Returns:
        LazyFrame: Polars lazy data frame.

    Raises:
        SystemExit: If data cannot be loaded.
    """
    if isinstance(source, dict):
        if source.get("query") and source.get("uri"):
            # Read from PostgreSQL database
            return pl.read_database_uri(
                query=source["query"], uri=source["uri"]
            ).lazy()
        elif "file_path" in source:
            # Get storage_options to read from cloud providers
            storage_options = source.get("storage_options")
            # Get schema_overrides to alter schema dtypes for csv / xlsx
            schema_overrides = source.get("schema_overrides")
            # Get read function if extension parameter was specified
            read = read_source_func.get(source.get("extension"))
            # If extension wasn't specified or read function wasn't found
            if read is None:
                return get_read_source_func(
                    source["file_path"], storage_options)
            else:
                return read(
                    source["file_path"],
                    storage_options=storage_options).lazy()
        else:
            raise SystemExit(f"Incorrect source specification: {source}")
    elif isinstance(source, str):
        return get_read_source_func(source)
    else:
        raise SystemExit("No source specification")


@exception_handler(exit_on_error=True)
def get_read_source_func(
        source: str,
        storage_options: dict = None
) -> pl.LazyFrame:
    """
    Get function to read the source based on its ending.

    This function iterates through `read_source_func` keys
    matching them with source extension. If there is match,
    it reads the source, otherwise SystemExit raised.

    Args:
        source (str): Data source name.
        storage_options (Dict[str, str]): Credentials to read from cloud providers.

    Returns:
        LazyFrame: Polars lazy data frame.

    Raises:
        SystemExit: If source extension didn't match `read_source_func` keys.
    """
    for extension, read in read_source_func.items():
        if source.endswith(extension):
            return read(source, storage_options=storage_options).lazy()
    # Raise exception if source ending didn't match supported file extensions
    raise SystemExit(f"Unsupported source: {source}")


@exception_handler()
def handle_schema_overrides(data) -> Dict[str, Any]:
    """
    
    """
    return data