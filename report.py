import os
import logging
from pathlib import Path
from typing import Dict, List, Union, Any
from polars import LazyFrame
from utils import plot_data


def make_report(
        df: LazyFrame,
        metadata: Dict[str, Union[List[str], str]],
        config: Dict[str, Any]
) -> None:
    """
    Generate a Markdown report with plots.

    Args:
        df (LazyFrame): The aggregated data.
        metadata (dict): Metainfo about aggregated data.
        config (dict): Configuration dictionary for making analysis and report.
    """
    output = config.get("output", "output")
    Path(output).mkdir(exist_ok=True)

    md_toc, md_content = [], []
    col = "__overview"
    plot_data(
        df["__date"],
        df["__count"],
        df["__balance"] if metadata.get("__balance") else None,
        file_path=f"{output}/{col}", config=config.get("plotly", {}),
        titles=("__count", metadata.get("__balance")))
    md_toc.append(("Overview", col))
    md_content.append(f"## <a name='{col}'></a> Overview\n")
    md_content.append(f"![{col}]({col}.png)\n\n")

    for col in metadata:
        plot_data(
            df['__date'],
            *[
                df[col + '_' + metadata[col]["common"][i]]
                for i in range(len(metadata[col]["common"]))
            ],
            file_path=f"{output}/{col}",
            config=config.get("plotly", {}),
            titles=metadata[col]["common"])
        md_toc.append((col, col))
        md_content.append(f"## <a name='{col}'></a> {col}\n")
        md_content.append(f"![{col}]({col}.png)\n")

        # Numeric datatypes if present
        if metadata[col].get("numeric"):
            plot_data(
                df['__date'],
                *[
                    df[col + '_' + metadata[col]["numeric"][i]]
                    for i in range(len(metadata[col]["numeric"]))
                ],
                file_path=f"{output}/{col}__numeric",
                config=config.get("plotly", {}),
                titles=metadata[col]["numeric"])
            md_content.append(f"### <a name='{col}'></a> {col} [numeric]\n")
            md_content.append(f"![{col}]({col}__numeric.png)\n")
        md_content.append('\n[Back to the `TOC`](#toc)\n\n')
    make_md(md_toc, md_content, output, config["source"])


def make_md(
        toc: List[str],
        content: List[str],
        output: str,
        source: str
) -> None:
    """
    Make a markdown file with table of contents and content.
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
