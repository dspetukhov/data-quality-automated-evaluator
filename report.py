import os
import logging
from pathlib import Path
from typing import Dict, List, Union, Any, Tuple
from polars import LazyFrame
from utils import plot_data


def make_report(
        df: LazyFrame,
        metadata: Dict[str, Union[Tuple[str], str]],
        config: Dict[str, Any]
) -> None:
    """
    Generate a Markdown report with plots.

    Args:
        df (LazyFrame): The aggregated data.
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
        df["__date"],
        df["__count"],
        df["__balance"] if metadata.get("__balance") else None,
        config=config.get("plotly", {}),
        file_path=f"{output}/{col}",
        titles=("__count", metadata.get("__balance")))
    md_toc.append(("Overview", col))
    md_content.append(f"## <a name='{col}'></a> Overview\n")
    md_content.append(f"![{col}]({col}.png)\n\n")
    md_content.append(make_md_table(stats))

    for col in metadata:
        stats = plot_data(
            df['__date'],
            *[
                df[col + '_' + metadata[col]["common"][i]]
                for i in range(len(metadata[col]["common"]))
            ],
            config=config.get("plotly", {}),
            file_path=f"{output}/{col}",
            titles=metadata[col]["common"])
        md_toc.append((col, col))
        md_content.append(f"## <a name='{col}'></a> `{col}`\n")
        # Predictive power if present
        # md_content.append(f"### <a name='{col}'></a> {col} [common]\n")
        md_content.append(f"![{col}]({col}.png)\n")
        md_content.append(make_md_table(stats))

        # Numeric datatypes if present
        if metadata[col].get("numeric"):
            plot_data(
                df['__date'],
                *[
                    df[col + '_' + metadata[col]["numeric"][i]]
                    for i in range(len(metadata[col]["numeric"]))
                ],
                config=config.get("plotly", {}),
                file_path=f"{output}/{col}__numeric",
                titles=metadata[col]["numeric"])
            md_content.append(
                f"### <a name='{col}'></a> [{metadata[col]['dtype']}]\n")
            md_content.append(f"![{col}]({col}__numeric.png)\n")
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


def make_md_table(data) -> str:
    """
    Make a markdown table from a dictionary.

    Args:
        data (dict): Dictionary with data to be converted to a table.

    Returns:
        str: Markdown table as an HTML code.
    """
    header, content = [], []
    for key in data.keys():
        header.append(f"<th>{key}</th>")
        for k, v in data[key].items():
            if k == "Range":
                v = f"{k}: {v['Max'] - v['Min']} | Min: {v['Min']} | Max: {v['Max']}"
            elif k == "IQR":
                v = f"{k}: {v['Q3'] - v['Q1']} | Q1: {v['Q1']} | Q3: {v['Q3']}"
            content.append(f"<tr><td>{v}</td></tr>\n")
    output = """
        <table style="width:100%; table-layout:fixed; border-collapse:collapse; border-bottom: 1px solid black;">
        <colgroup>
            <col style="width:50%; border:5px solid transparent">
            <col style="width:50%">
        </colgroup>
        <tr>{header}</tr>
        {content}
        </table>
    """.format(
        header="".join(header),
        content="".join(content))
    return "".join([
        line.lstrip() for line in output.splitlines()
    ]) + "\n\n"
