import os
from tabulate import tabulate
from pathlib import Path
from typing import Dict, List, Union, Any, Tuple
from polars import DataFrame
from utils import plot_data
from analysis import exception_handler


@exception_handler()
def make_report(
        df: DataFrame,
        metadata: Dict[str, Union[Tuple[str], str]],
        config: Dict[str, Any]
) -> None:
    """
    Generate a markdown report with plots and tables.

    This function handles the report generation by processing input data
    for plotting, collecting and producing output as a single markdown file
    taking into account settings specified in the configuration file.

    Args:
        df (DataFrame): Aggregated data for report assembling.
        metadata (Dict[str, Union[Tuple[str], str]]): Metainformation describing aggregated data.
        config (Dict[str, Any]): Configuration dictionary specifying
            data source name, markdown options, and plotting options.

    Returns:
        None: Function writes the report to disk.
    """
    output = config.get(
        "output",
        Path(config["source"]).name.split(".")[0]
    )
    Path(output).mkdir(exist_ok=True)  # Creates output directory if not exists

    # Markdown configuration
    precision = config.get("markdown", {}).get("float_precision")
    style = config.get("markdown", {}).get("css_style")
    md_toc = []
    md_content = [
        f"<link rel='stylesheet' href='{style}'>\n" if style else ""]

    # Plotly configuration
    plotly_config = config.get("plotly", {})
    plotly_config.update(config.get("anomalies", {}))

    # Create overview plot and get evaluation from input data
    col = "__overview"
    stats = plot_data(
        df["__date"],
        df["__count"],
        df["__target"] if "__target" in df.columns else None,
        config=plotly_config,
        file_path=f"{output}/{col}",
        titles=(
            "Number of values",
            "Target average" if "__target" in df.columns else None)
    )
    collect_md_content(col, stats, md_toc, md_content, precision)

    # Create plots and get evaluations for each column in metadata
    mapping = config.get("mapping", {})
    for col in metadata:
        stats = plot_data(
            df["__date"],
            *[
                df[col + metadata[col]["common"][i]]
                for i in range(len(metadata[col]["common"]))
            ],
            config=plotly_config,
            file_path=f"{output}/{col}",
            titles=[mapping.get(el, el) for el in metadata[col]["common"]]
        )
        collect_md_content(col, stats, md_toc, md_content, precision)

        # Continue if datatype is numeric
        if metadata[col].get("numeric"):
            stats = plot_data(
                df["__date"],
                *[
                    df[col + metadata[col]["numeric"][i]]
                    for i in range(len(metadata[col]["numeric"]))
                ],
                config=plotly_config,
                file_path=f"{output}/{col}__numeric",
                titles=[mapping.get(el, el) for el in metadata[col]["numeric"]]
            )
            collect_md_content(
                col, stats,
                md_toc, md_content,
                suffix="__numeric",
                precision=precision,
                dtype=metadata[col]["dtype"])

        md_content.append("[Back to the TOC](#toc)\n")

    # Write collected markdown content as a markdown file
    write_md_file(
        md_toc, md_content,
        output, config["source"],
        config.get("markdown", {}).get("name"))


@exception_handler()
def collect_md_content(col, data, toc, content, precision=4, **kwargs) -> None:
    """
    Process data to create markdown content
    by updating table-of-contents and content lists.

    This function appends new entry to the table-of-contents list
    and appends formatted markdown string to the content list.

    Args:
        col (str): Section name.
        data (List[dict]): Data to create a table using `make_md_table`.
        toc (List): List to be updated with new table-of-contents entries.
        content (List): List to be updated with new markdown content.
        precision (int, optional): Number of decimal places to format numbers.
        **kwargs: Additional keyword arguments for numeric data types
            to alter content formation logic.

    Returns:
        None: Function updates lists in-place.
    """
    alias = kwargs.get("dtype", "Overview" if col == "__overview" else col)
    suffix = kwargs.get("suffix", "")

    if not suffix:
        # Add new section to the table-of-contents with anchor
        toc.append((col, alias))

    content.append((
        "{level} <a name='{col}'></a> `{alias}`\n"  # New section with anchor
        "![{col}]({col}{suffix}.png)\n"  # Embed plot
        "{table}"  # Embed table based on input data
    ).format(
        level="###" if suffix else "##",
        col=col,
        alias=alias,
        suffix=suffix,
        table=make_md_table(data, precision)
    ))


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
    sample = next((el for el in data if el))
    # Compose formatted table content
    rows = [
        [" " if key == "title" else f"**{key}**"] +
        [format_number(el.get(key, ""), precision) for el in data]
        for key in sample
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
        toc: List[Tuple[str, str]],
        content: List[str],
        output: str,
        source: str,
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
        toc (List[Tuple[str, str]]): List of tuples (section name, anchor)
            for the table-of-contents.
        content (List[str]): List of strings for each markdown section.
        output (str): Output directory path where markdown file will be saved.
        source (str): Source file path to be referenced in the report.
        file_name (str): Name of the markdown file.

    Returns:
        None: Function writes the markdown file to disk.
    """
    # Create `Table of contents` with links
    toc = "\n".join([
        f"- [{section}](#{anchor})"
        for anchor, section in toc
    ])
    content = "\n".join(content)
    # Adjust file_name if it is not None
    if file_name and not file_name.endswith(".md"):
        file_name += ".md"
    # Assemble content and write it to file
    with open(
        os.path.join(output, file_name or "README.md"), "w", encoding="utf-8"
    ) as f:
        f.writelines([
            f"# Preliminary analysis for **`{source}`**\n\n",
            f"## Table of contents<a name='toc'></a>\n{toc}\n\n",
            content
        ])


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
