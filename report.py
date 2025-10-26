import os
from tabulate import tabulate
from pathlib import Path
from typing import Dict, List, Union, Any, Tuple
from polars import LazyFrame
from utils import plot_data
from analysis import exception_handler


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
        f"<link rel='stylesheet' href='{style}'>\n\n"
        if style else ""]
    print(md_content)
    # plotly definitions
    plotly_config = config.get("plotly", {})
    plotly_config.update(config.get("anomalies", {}))

    col = "__overview"
    stats = plot_data(
        lf["__date"],
        lf["__count"],
        lf["__balance"] if "__balance" in lf.columns else None,
        config=plotly_config,
        file_path=f"{output}/{col}",
        titles=(
            "Number of values",
            "Class balance" if "__balance" in lf.columns else None)
    )
    md_toc.append(("Overview", col))
    md_content.append(f"## <a name='{col}'></a> Overview\n")
    md_content.append(f"![{col}]({col}.png)\n")
    md_content.append(make_md_table(stats, precision))

    mapping = config.get("mapping", {})
    for col in metadata:
        stats = plot_data(
            lf['__date'],
            *[
                lf[col + metadata[col]["common"][i]]
                for i in range(len(metadata[col]["common"]))
            ],
            config=plotly_config,
            file_path=f"{output}/{col}",
            titles=[mapping.get(el, el) for el in metadata[col]["common"]])
        md_toc.append((col, col))
        md_content.append(f"## <a name='{col}'></a> `{col}`\n")
        md_content.append(f"![{col}]({col}.png)\n")
        md_content.append(make_md_table(stats, precision))

        # Numeric datatypes if present
        if metadata[col].get("numeric"):
            stats = plot_data(
                lf['__date'],
                *[
                    lf[col + metadata[col]["numeric"][i]]
                    for i in range(len(metadata[col]["numeric"]))
                ],
                config=plotly_config,
                file_path=f"{output}/{col}__numeric",
                titles=[mapping.get(el, el) for el in metadata[col]["numeric"]])
            md_content.append(
                f"### <a name='{col}'></a> [{metadata[col]['dtype']}]\n")
            md_content.append(f"![{col}]({col}__numeric.png)\n")
            md_content.append(
                make_md_table(stats, precision)
            )
        md_content.append('\n[Back to the TOC](#toc)\n\n')
    make_md(md_toc, md_content, output, config["source"])


@exception_handler()
def make_md(
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
    toc = "".join([
        f"- [{section}](#{anchor})\n"
        for section, anchor in toc
    ])
    with open(os.path.join(output, "README.md"), "w") as f:
        f.write(  # possible exception
            "# Preliminary analysis for **`{}`**\n\n".format(source) +
            "<a name='toc'></a>\n" + "".join(toc) + "\n" +
            "".join(content))


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
    ) + "\n\n"


def format_number(value, precision=4):
    """Format float numbers to the specified precision."""
    if isinstance(value, float):
        if len(str(value).split(".")[1]) > precision:
            value = f"{value:,.{precision}f}"
    elif isinstance(value, tuple):
        value = " ± ".join([f"{v:,.{precision}f}" for v in value])
    return str(value)
