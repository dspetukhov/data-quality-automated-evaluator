import os
import polars as pl
from typing import Any, Dict, Union
from .handle_exceptions import exception_handler
from .setup_logging import logging


@exception_handler(exit_on_error=True)
def read_source(source: Dict[str, str]) -> pl.LazyFrame:
    """
    Read specified source of data as Polars LazyFrame.

    Supports CSV, Parquet, Iceberg, XLSX, and database URIs.
    Returns LazyFrame for preprocessing. If loading fails,
    it terminates the main program.
    In case of reading from cloud providers `storage_options` definition
    is expected, otherwise Polars will try to infer credentials implicitly
    from environment variables, e.g. AWS_REGION in case of S3.
    `uri` and `storage_options` values can be read from environment variables
    if they are specified with a $ sign at the beginning.

    Args:
        source (Dict[str, str]): Data source specification.
            Can be a dictionary with `query` and `uri` keys
            to read from a PostgreSQL database,
            or a dictionary with `file_path` key
            to read from file, Google Drive, S3
            (`storage_options` and `file_format` are optional).

    Returns:
        pl.LazyFrame: Polars lazy data frame.

    Raises:
        SystemExit: If data cannot be loaded.
    """
    if isinstance(source, dict):
        if source.get("query") and source.get("uri"):
            # Read from PostgreSQL database
            lf = pl.read_database_uri(
                query=source["query"],
                uri=handle_environment_variables(source["uri"])
            ).lazy()

        elif source.get("file_path"):
            # Get storage_options to read from cloud providers
            storage_options = handle_environment_variables(
                source.get("storage_options")
            )
            # Get schema_overrides to alter schema dtypes for csv / xlsx
            schema_overrides = handle_schema_overrides(
                source.get("schema_overrides"))
            # Get file format if specified
            file_format = source.get("file_format")
            lf = _read_source(
                    source["file_path"],
                    file_format,
                    storage_options, schema_overrides)

        return lf

    raise SystemExit(f"Incorrect source specification: {source}")


@exception_handler(exit_on_error=True)
def _read_source(
        source: str,
        file_format: str,
        storage_options: Dict[str, str],
        schema_overrides: Dict[str, str]
) -> pl.LazyFrame:
    """
    Read source based on file format specified or based on source name ending.

    This function selects suitable read function
    based on file format specified. If it wasn't specified,
    it selects read function by matching source name ending
    with supported file formats. If read function is found,
    it reads source as Lazy data frame, otherwise SystemExit raised.

    Args:
        source (str): Data source name.
        file_format (str): File format specified in configuration.
        storage_options (Dict[str, str]): Credentials to read from cloud providers.
        schema_overrides (Dict[str, str]): Alters data types for specific columns
            during schema inference.

    Returns:
        pl.LazyFrame: Polars lazy data frame.

    Raises:
        SystemExit: If there is no read function for the file format provided.
    """
    # {file format: read function} mapping
    read_source_func = {
        "csv": pl.scan_csv,
        "parquet": pl.scan_parquet,
        "iceberg": pl.scan_iceberg,
        "xlsx": pl.read_excel
    }
    if file_format in read_source_func:
        read_func = read_source_func[file_format]
    else:
        # Try to match source ending with supported file formats
        for ff, rf in read_source_func.items():
            if source.endswith(ff):
                logging.info(f"Identified file format: {ff}")
                file_format, read_func = ff, rf

    if file_format in ("csv", "xlsx"):
        lf = read_func(
            source,
            storage_options=storage_options,
            schema_overrides=schema_overrides
        ).lazy()
    elif file_format in read_source_func:
        lf = read_func(source, storage_options=storage_options).lazy()
    else:
        raise SystemExit(f"Unsupported source: {source}")

    return lf


@exception_handler()
def handle_schema_overrides(data: Dict[str, str]) -> Dict[str, Any]:
    """
    Replace string data type representation into Polars data type.

    Args:
        data (Dict[str, str]): Dict of types representation
            to be mapped with Polars data types.

    Returns:
        Dict[str, Any]: Mapping of types representation to Polars data types.
    """
    dtypes = {
        "String": pl.String,
        "Date": pl.Date,
        "Datetime": pl.Datetime
    }
    if isinstance(data, dict):
        data = {key: dtypes[value] for key, value in data.items()}

    return data


@exception_handler()
def handle_environment_variables(
    params: Union[str, Dict[str, str]]
) -> Union[str, Dict[str, str]]:
    """
    Replace environment variable placeholders with actual values.

    "$" sign as a first symbol in placeholders is expected
    to search and replace placeholders with actual values.

    Args:
        params (Union[str, Dict[str, str]]): Input parameters potentially
            containing environment variable placeholders.

    Returns:
        Union[str, Dict[str, str]]: Updated parameters
            with environment variable placeholders replaced by their actual values.
    """
    if isinstance(params, str) and params.startswith("$"):
        params = params[1:]
        if params in os.environ:
            logging.info(f"Environment variable for {params} is found")
            params = os.getenv(params)
    elif isinstance(params, dict):
        for key, value in params.items():
            if value.startswith("$"):
                value = value[1:]
                if value in os.environ:
                    logging.info(f"Environment variable for {value} is found")
                    params[key] = os.getenv(value)
    return params
