import os
import logging
from pathlib import Path
from typing import Dict, List, Union, Any, Tuple
from polars import LazyFrame
from utils import plot_data


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

    md_toc, md_content = [], []
    col = "__overview"
    stats = plot_data(
        lf["__date"],
        lf["__count"],
        lf["__balance"] if "__balance" in lf.columns else None,
        config=config.get("plotly", {}),
        file_path=f"{output}/{col}",
        titles=("Number of values", "Class balance" if "__balance" in lf.columns else None))
    md_toc.append(("Overview", col))
    md_content.append(f"## <a name='{col}'></a> Overview\n")
    md_content.append(f"![{col}]({col}.png)\n\n")
    md_content.append(make_md_table(stats, config.get("markdown", {})))

    mapping = config.get("mapping", {})
    for col in metadata:
        stats = plot_data(
            lf['__date'],
            *[
                lf[col + metadata[col]["common"][i]]
                for i in range(len(metadata[col]["common"]))
            ],
            config=config.get("plotly", {}),
            file_path=f"{output}/{col}",
            titles=[mapping.get(el, el) for el in metadata[col]["common"]])
        md_toc.append((col, col))
        md_content.append(f"## <a name='{col}'></a> `{col}`\n")s
        md_content.append(f"![{col}]({col}.png)\n")
        md_content.append(make_md_table(stats, config.get("markdown", {})))

        # Numeric datatypes if present
        if metadata[col].get("numeric"):
            stats = plot_data(
                lf['__date'],
                *[
                    lf[col + metadata[col]["numeric"][i]]
                    for i in range(len(metadata[col]["numeric"]))
                ],
                config=config.get("plotly", {}),
                file_path=f"{output}/{col}__numeric",
                titles=[mapping.get(el, el) for el in metadata[col]["numeric"]])
            md_content.append(
                f"### <a name='{col}'></a> [{metadata[col]['dtype']}]\n")
            md_content.append(f"![{col}]({col}__numeric.png)\n")
            md_content.append(
                make_md_table(stats, config.get("markdown", {}))
            )
        md_content.append('\n[Back to the TOC](#toc)\n\n')
    make_md(md_toc, md_content, output, config["source"])


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
    try:
        with open(os.path.join(output, "README.md"), "w") as f:
            f.write(
                "# Preliminary analysis for **`{}`**\n\n".format(source) +
                "<a name='toc'></a>\n" + "".join(toc) + "\n" +
                "".join(content))
    except IOError as e:
        logging.error(f"Failed to write file: {e}")


def make_md_table(data, config) -> str:
    """
    Create a markdown table from a dictionary with calculated statistics.

    Args:
        data (dict): Dictionary to be converted to a table.

    Returns:
        str: Markdown table as an HTML code.
    """
    data_restructured = {
        k: [item.get(k, "") for item in data]
        for item in data for k in item
    }

    idx_width = config.get("index_column", 10)
    style = config.get("html_style", "")
    precision = config.get("float_precision")

    header, col_width = make_md_table_header(
        data_restructured, style, idx_width)

    content = []
    for key, values in data_restructured.items():
        sample_value = next((val for val in values if val), None)
        if isinstance(sample_value, (str, tuple)):
            content.append(
                make_md_table_row(key, values, style, precision)
            )
        elif isinstance(sample_value, dict):
            content.append(
                make_md_table_from_dict(key, values, style, precision)
            )

    colgroup = f"<col style='width:{idx_width}%'>" + "".join([
        f'<col style="width:{col_width}%">'
        for _ in range(len(data))
    ])
    output = f"""<table style="width:100%;table-layout:fixed;border-collapse:collapse;border:1px solid #ddd;margin:5px 0;">
    <colgroup>
        {colgroup}
    </colgroup>
    <thead style="background-color:#f5f5f5;">
        {header}
    </thead>
    <tbody>
        {"".join(content)}
    </tbody>
    </table>"""
    return output + "\n\n"


def make_md_table_header(data, style, width):
    """Create a HTML header for markdown table.

    Args:
        data (dict): Dictionary to be converted to a table.
        style (str): HTML style for Markdown table.
        width (int): relative index column width.

    Returns:
        tuple (str, int): header as HTML string and relative column width (%).
    """
    header = [f"<th style='{style};width:{width}%'></th>"]
    col_width = (100 - width) // len(data)
    titles = data.pop("title")
    for title in titles:
        header.append(
            f"<th style='{style};width:{col_width}%'>{title}</th>"
        )
    return "<tr>{}</tr>".format("".join(header)), col_width


def make_md_table_row(key, values, style, precision):
    """Create HTML-formatted Markdown table row for simple variables."""
    content = [f"<td style='{style};font-weight:bold'>{key}</td>"]
    content.extend([
        f"<td style='{style}'>{format_number(val, precision)}</td>"
        for val in values
    ])
    return "<tr>{}</tr>".format("".join(content))


def make_md_table_from_dict(key, values, style, precision):
    """Create HTML-formatted Markdown table rows from dictionary values."""
    content = []
    values_restructured = {
        k: [(item or {}).get(k, "") for item in values]
        for item in values for k in item
    }

    for item in values_restructured:
        content.append(
            make_md_table_row(
                f"{key} [{item}]", values_restructured[item], style, precision)
        )
    if key in ("Range", "IQR"):
        values = [
            list(v.values())[1] - list(v.values())[0]
            if v else "" for v in values
        ]
        content.append(
            make_md_table_row(f"{key}", values, style, precision)
        )
    return "<tr>{}</tr>".format("".join(content))


def format_number(value, precision=4):
    """Format float numbers to the specified precision."""
    if isinstance(value, float):
        if len(str(value).split(".")[1]) > precision:
            return f"{value:,.{precision}f}"
    elif isinstance(value, tuple):
        return " ± ".join([f"{v:,.{precision}f}" for v in value])
    return str(value)
