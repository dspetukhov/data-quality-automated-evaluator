import os
from tabulate import tabulate
from pathlib import Path
from typing import Dict, List, Union, Any, Tuple
from polars import LazyFrame
from utils import plot_data
from analysis import exception_handler


@exception_handler()
def make_report(
        lf: LazyFrame,
        metadata: Dict[str, Union[Tuple[str], str]],
        config: Dict[str, Any]
) -> None:
    """
    Generate a Markdown report with plots.

    Args:
        lf (LazyFrame): The aggregated data.
        metadata (dict): Metainfo about aggregated data.
        config (dict): Configuration dictionary for making analysis and report.

    Returns:
        None
    """
    output = config.get("output", "output")
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
    collect_md(col, stats, md_toc, md_content, precision)

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
        collect_md(col, stats, md_toc, md_content, precision)

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
            collect_md(
                col, stats,
                md_toc, md_content,
                suffix="__numeric",
                precision=precision,
                dtype=metadata[col]["dtype"])

        md_content.append("[Back to the TOC](#toc)\n")
    write_md(md_toc, md_content, output, config["source"])


@exception_handler(exit_on_error=True)
def write_md(
        toc: List[Tuple[str, str]],
        content: List[str],
        output: str,
        source: str
) -> None:
    """
    Make and write a markdown file with table of contents and content.

    Args:
        toc (list): List of tuples (section name, anchor).
        content (list): List of markdown content strings.
        output (str): Output directory path.
        source (str): Source file path.

    Returns:
        None
    """
    toc = [
        f"- [{section}](#{anchor})"
        for anchor, section in toc
    ]
    with open(os.path.join(output, "README.md"), "w") as f:
        f.write(  # possible exception
            "# Preliminary analysis for **`{}`**\n\n".format(source) +
            "<a name='toc'></a>\n" + "\n".join(toc) + "\n\n" +
            "\n".join(content))


def collect_md(col, data, toc, content, precision=4, **kwargs) -> List:
    """Process data to create Markdown file
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
    Create a markdown table from a dictionary with calculated statistics.

    Args:
        data (dict): Dictionary to be converted to a table.

    Returns:
        str: Markdown table as an HTML code.
    """
    sample = next((el for el in data if el))
    rows = [
        [" " if key == "title" else f"**{key}**"] + [format_number(el[key], precision) for el in data]
        for key in sample
    ]
    return tabulate(
        rows,
        headers="firstrow",
        tablefmt="pipe",
        colalign=["left"] + ["center"]*(len(rows[0])-1)
    ) + "\n"


def format_number(value, precision):
    """Format float numbers with specified precision."""
    if isinstance(value, float):
        if len(str(value).split(".")[1]) > precision:
            value = f"{value:,.{precision}f}"
    elif isinstance(value, tuple):
        value = " ± ".join([f"{v:,.{precision}f}" for v in value])
    return str(value)
