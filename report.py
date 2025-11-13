import os
from typing import Dict, List, Union, Any, Tuple
from polars import DataFrame
from pathlib import Path
from utility import exception_handler
from evaluate import evaluate_data
from plot import plot_data
from tabulate import tabulate


@exception_handler()
def make_report(
        df: DataFrame,
        metadata: Dict[str, Union[Tuple[str], str]],
        config: Dict[str, Any]
) -> None:
    """
    Generate markdown report with plots and tables.

    This function handles the report generation by processing input data
    for plotting, collecting and producing output as a single markdown file
    taking into account parameters specified in the configuration file.

    Args:
        df (DataFrame): Aggregated data for report assembling.
        metadata (Dict[str]): Dict of aggregated columns
            indicating types for numeric columns.
        config (Dict[str, Any]): Configuration dictionary specifying
            data source name, markdown options, and plotting options.

    Returns:
        None: Function writes the report to disk.
    """
    # Get key variables to make the report
    output, content, precision, outliers, plotly = get_report_variables(config)

    data_evals = {}
    # Get evaluations and create overview plot for columns
    # representing general aggregations of source data:
    # number of values and target average
    data = df.select(
        ["__date"] + [
            item for item in df.columns if item.startswith(" __")]
    )
    col = "__overview"
    # Evaluate data
    evals, bounds = evaluate_data(data, outliers)
    data_evals[col] = {"evals": evals}
    # Plot data
    plot_data(data, bounds=bounds, config=plotly, file_path=f"{output}/{col}")

    # Get evaluations and create plots for columns
    # representing aggregations for a column in source data:
    # number of unique values and ratio of null values
    for col in metadata:
        data = df.select(
            ["__date"] + [
                item for item in df.columns
                if item.startswith(f"__ {col} __")]
        )
        evals, bounds = evaluate_data(data, outliers)
        data_evals[col] = {"evals": evals}
        plot_data(
            data,
            bounds=bounds,
            config=plotly,
            file_path=f"{output}/{col}")

        # Get evaluations and create plots for columns
        # representing extra aggregations for a numeric column in source data:
        # minimum, maximum, mean, median, and standard deviation
        if metadata.get(col):
            data = df.select(
                ["__date"] + [
                    item for item in df.columns
                    if item.startswith(f"n__ {col} __")]
            )
            evals, bounds = evaluate_data(data, outliers)
            data_evals[col].update(
                {"evals_numeric": evals, "dtype": metadata[col]})
            plot_data(
                data,
                bounds=bounds,
                config=plotly,
                file_path=f"{output}/{col}__numeric")

    # Collect markdown content
    content = collect_md_content(
        data_evals, content, config["source"], precision)

    # Write content as a markdown file
    write_md_file(content, output, config.get("markdown", {}).get("name"))


@exception_handler()
def get_report_variables(config: Dict[str, Any]):
    """
    Get key variables to make the report using the configuration provided.

    This function creates variables necessary for making markdown report
    based on the specified configuration. They include: output directory name,
    style for markdown tables, precision to format floats in markdown tables,
    outliers detection parameters, Plotly parameters for plots.

    Args:
        config (Dict[str, Any]): Configuration dictionary.

    Returns:
        Tuple(Any):
            - Output directory to store report file and plots.
            - Precision to format floats in markdown tables.
            - Outliers detection parameters.
            - Plotly configration for plots.
    """
    # Determine the name of the output directory using `output` parameter
    # in configuration or using the name of the source without extension
    output_dir = config.get(
        "output",
        Path(config["source"]).name.split(".")[0]
    )
    # Create output directory
    Path(output_dir).mkdir(exist_ok=True)

    # Style for markdown tables
    style = config.get("markdown", {}).get("css_style")
    md_content = [
        f"<link rel='stylesheet' href='{style}'>\n" if style else ""]

    # Number of decimal places to format numbers in markdown tables
    precision = config.get("markdown", {}).get("float_precision")

    # Outliers detection parameters
    outliers_config = config.get("outliers", {})

    # Plotly configuration
    plotly_config = config.get("plotly", {})

    return output_dir, md_content, precision, outliers_config, plotly_config


@exception_handler()
def collect_md_content(
    data: Dict[str, Any],
    content: List[str],
    source: str,
    precision: int = 4
) -> List[str]:
    """
    Process data to create markdown content
    by updating table-of-contents and content lists.

    This function appends new entry to the table-of-contents list
    and appends formatted markdown string to the content list.

    Args:
        data (Dict[str, Any]): Data to create a table using `make_md_table`.
        content (List[str]): List with markdown table style string.
        precision (int, optional): Number of decimal places to format numbers.

    Returns:
        List[str]: List of strings to be written in file.
    """
    toc = []
    for col in data:
        # Get section title (`alias`)
        alias = "Overview" if col == "__overview" else col

        if not data[col].get("dtype"):
            # Add new section to the table-of-contents with anchor
            toc.append(f"- [{alias}](#{col})")

        # Add new entry to the content: section with anchor, plot, and table
        content.append((
            "## <a name='{col}'></a> `{alias}`\n"
            "![{col}]({col}.png)\n"
            "{table}"
        ).format(
            col=col, alias=alias,
            table=make_md_table(data[col]["evals"], precision))
        )
        # Add extra section for numeric columns
        if data[col].get("dtype"):
            content.append((
                "### <a name='{col}'></a> `{alias}`\n"
                "![{col}]({col}__numeric.png)\n"
                "{table}"
            ).format(
                col=col, alias=data[col]["dtype"],
                table=make_md_table(data[col]["evals_numeric"], precision))
            )
        # Add backlink to the Table-of-contents
        content.append("[Back to the TOC](#toc)\n")

    toc = "\n".join(toc)
    content = "\n".join(content)

    md_output = [
        f"# Preliminary analysis for **`{source}`**\n\n",
        f"## Table of contents<a name='toc'></a>\n{toc}\n\n",
        content
    ]
    return md_output


@exception_handler()
def make_md_table(data, precision) -> str:
    """
    Create a markdown table from input data.

    This function converts a list of dictionaries
    into a markdown table using the tabulate library.
    The table is returned as a string
    to be included as a part of the markdown report.

    Args:
        data (List[dict]): List of dictionaries with calculated statistics.
        precision (int): Number of decimal places to format numbers.

    Returns:
        str: Markdown table.
    """
    # Compose formatted table content
    rows = [
        [" " if key == "title" else f"**{key}**"] +
        [format_number(el.get(key, ""), precision) for el in data]
        for key in data[0]
    ]
    # Create markdown table using `tabulate`
    return tabulate(
        rows,
        headers="firstrow",
        tablefmt="pipe",
        colalign=["left"] + ["center"]*(len(rows[0])-1)
    ) + "\n"


@exception_handler(exit_on_error=True)
def write_md_file(
        content: List[str],
        output: str,
        file_name: str = None
) -> None:
    """
    Create the markdown report file.

    This function creates a markdown file that includes a table-of-contents
    (TOC) and the main content sections.
    The name of the source data is included for reference.
    The file is written to the specified output directory.
    The name of the file is defined by file_name variable,
    defaults to README.md.

    Args:
        content (List[str]): List of strings for each markdown section.
        output (str): Output directory path where markdown file will be saved.
        file_name (str): Name of the markdown file.

    Returns:
        None: Function writes markdown file to disk.
    """
    # Adjust file_name if it is not None
    if file_name and not file_name.endswith(".md"):
        file_name += ".md"
    # Write final content string to file
    with open(
        os.path.join(output, file_name or "README.md"), "w", encoding="utf-8"
    ) as f:
        f.writelines(content)


@exception_handler()
def format_number(value, precision):
    """
    Format float numbers with specified precision.

    This function formats a float or a tuple of floats to a string with
    the given number of decimal places,
    otherwise it returns it as a string unchanged.

    Args:
        value (Union[float, tuple]): Number or a tuple of numbers to format.
        precision (int): Number of decimal places to format numbers.

    Returns:
        str: Formatted number(s) as a string.
    """
    if isinstance(value, float):
        if len(str(value).split(".")[1]) > precision:
            value = f"{value:,.{precision}f}"
    elif isinstance(value, tuple):
        value = " ± ".join([f"{v:,.{precision}f}" for v in value])
    return str(value)
