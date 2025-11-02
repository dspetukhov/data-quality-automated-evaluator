import os
from pathlib import Path
from typing import Dict, Union, Any, Tuple, List
from polars import LazyFrame
from utils import plot_data
from utility import exception_handler
from tabulate import tabulate


@exception_handler()
def make_report(
        lf: LazyFrame,
        metadata: Dict[str, Union[Tuple[str], str]],
        config: Dict[str, Any]
) -> None:
    """
    Generate a markdown report with plots.

    Args:
        lf (LazyFrame): The aggregated data.
        metadata (dict): Metainfo about aggregated data.
        config (dict): Configuration dictionary for making analysis and report.

    Returns:
        None
    """
    output = config.get(
        "output",
        Path(config["source"]).name.split(".")[0]
    )
    Path(output).mkdir(exist_ok=True)

    # markdown definitions
    precision = config.get("markdown", {}).get("float_precision")
    style = config.get("markdown", {}).get("css_style")
    md_toc = []
    md_content = [
        f"<link rel='stylesheet' href='{style}'>\n"
        if style else ""]

    # plotly definitions
    plotly_config = config.get("plotly", {})
    plotly_config.update(config.get("anomalies", {}))

    col = "__overview"
    stats = plot_data(
        lf["__date"],
        lf["__count"],
        lf["__target"] if "__target" in lf.columns else None,
        config=plotly_config,
        file_path=f"{output}/{col}",
        titles=(
            "Number of values",
            "Target average" if "__target" in lf.columns else None)
    )
    collect_md_file(col, stats, md_toc, md_content, precision)

    mapping = config.get("mapping", {})
    for col in metadata:
        stats = plot_data(
            lf["__date"],
            *[
                lf[col + metadata[col]["common"][i]]
                for i in range(len(metadata[col]["common"]))
            ],
            config=plotly_config,
            file_path=f"{output}/{col}",
            titles=[mapping.get(el, el) for el in metadata[col]["common"]]
        )
        collect_md_file(col, stats, md_toc, md_content, precision)

        # Numeric datatypes if present
        if metadata[col].get("numeric"):
            stats = plot_data(
                lf["__date"],
                *[
                    lf[col + metadata[col]["numeric"][i]]
                    for i in range(len(metadata[col]["numeric"]))
                ],
                config=plotly_config,
                file_path=f"{output}/{col}__numeric",
                titles=[mapping.get(el, el) for el in metadata[col]["numeric"]]
            )
            collect_md_file(
                col, stats,
                md_toc, md_content,
                suffix="__numeric",
                precision=precision,
                dtype=metadata[col]["dtype"])

        md_content.append("[Back to the TOC](#toc)\n")
    write_md_file(md_toc, md_content, output, config["source"])


@exception_handler()
def collect_md_file(col, data, toc, content, precision=4, **kwargs) -> List:
    """Process data to create markdown file
    by updating lists for TOC and content in-place."""
    alias = kwargs.get("dtype", "Overview" if col == "__overview" else col)
    suffix = kwargs.get("suffix", "")

    if not suffix:
        toc.append((col, alias))

    content.append((
        "{level} <a name='{col}'></a> `{alias}`\n"
        "![{col}]({col}{suffix}.png)\n"
        "{table}"
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
        precision (int): Number of decimal places to format numbers to.

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
        source: str
) -> None:
    """
    Create the markdown report file.

    This function creates a markdown file that includes a table of contents
    (TOC) and the main content sections.
    The file is written to the specified output directory.
    The name of the source data is included for reference.

    Args:
        toc (List[Tuple[str, str]]): List of tuples (section name, anchor)
            for the table of contents.
        content (List[str]): List of strings for each markdown section.
        output (str): Output directory path where markdown file will be saved.
        source (str): Source file path to be referenced in the report.

    Returns:
        None: Function writes the markdown file to disk.
    """
    # Create `Table of contents` with links
    toc = "\n".join([
        f"- [{section}](#{anchor})"
        for anchor, section in toc
    ])
    content = "\n".join(content)
    # Assemble content and write it to file
    with open(os.path.join(output, "README.md"), "w") as f:
        f.write(f"""
            # Preliminary analysis for **`{source}`**\n
            ## Table of contents<a name='toc'></a>\n{toc}\n
            {content}
        """)


@exception_handler()
def format_number(value, precision):
    """
    Format float numbers with specified precision.

    This function formats a float or a tuple of floats to a string with
    the given number of decimal places,
    otherwise it returns it as a string unchanged.

    Args:
        value (Union[float, tuple]): Number or a tuple of numbers to format.
        precision (int): Number of decimal places to format numbers to.

    Returns:
        str: Formatted number(s) as a string.
    """
    if isinstance(value, float):
        if len(str(value).split(".")[1]) > precision:
            value = f"{value:,.{precision}f}"
    elif isinstance(value, tuple):
        value = " ± ".join([f"{v:,.{precision}f}" for v in value])
    return str(value)
