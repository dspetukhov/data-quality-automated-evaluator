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
        titles=("Number of values", metadata.get("__balance")))
    md_toc.append(("Overview", col))
    md_content.append(f"## <a name='{col}'></a> Overview\n")
    md_content.append(f"![{col}]({col}.png)\n\n")
    md_content.append(make_md_table(stats))

    mapping = config.get("mapping", {})
    for col in metadata:
        stats = plot_data(
            df['__date'],
            *[
                df[col + metadata[col]["common"][i]]
                for i in range(len(metadata[col]["common"]))
            ],
            config=config.get("plotly", {}),
            file_path=f"{output}/{col}",
            titles=[mapping.get(el, el) for el in metadata[col]["common"]])
        md_toc.append((col, col))
        md_content.append(f"## <a name='{col}'></a> `{col}`\n")
        # Predictive power if present
        # md_content.append(f"### <a name='{col}'></a> {col} [common]\n")
        md_content.append(f"![{col}]({col}.png)\n")
        md_content.append(make_md_table(stats))

        # Numeric datatypes if present
        if metadata[col].get("numeric"):
            stats = plot_data(
                df['__date'],
                *[
                    df[col + metadata[col]["numeric"][i]]
                    for i in range(len(metadata[col]["numeric"]))
                ],
                config=config.get("plotly", {}),
                file_path=f"{output}/{col}__numeric",
                titles=[mapping.get(el, el) for el in metadata[col]["numeric"]])
            md_content.append(
                f"### <a name='{col}'></a> [{metadata[col]['dtype']}]\n")
            md_content.append(f"![{col}]({col}__numeric.png)\n")
            md_content.append(make_md_table(stats))
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
    ld = len(data)
    _width = 10
    width = (100 - _width) // len(data)
    data = {
        key: [d.get(key, "") for d in data]
        for key in next((el for el in data if el is not None))
    }
    print(data, len(data))
    print()
    header = [f"<th style='text-align:center; width:{_width}%'></th>"]
    content = []
    for key, value in data.items():
        if key == "title":
            header.extend([
                f"<th style='text-align:center; width:{width}%'>{el}</th>"
                for el in value
            ])
            continue
        el = next((el for el in value if el is not None))
        if isinstance(el, str):
            _content = [f"<td style='text-align:center; padding:5px; font-weight:bold'>{key}</td>"]
            _content.extend([
                f"<td style='text-align:center; padding:5px'>{item}</td>"
                for item in value
            ])
            content.append("".join(_content))
        else:
            for k in el:
                _content = [f"<td style='text-align:center; padding:5px'>{k}</td>"]
                _content.extend([
                    f"<td style='text-align:center; padding:5px'>{v}</td>"
                    for v in
                    [
                        item.get(k) if isinstance(item, dict) else ""
                        for item in value
                    ]
                ])
                content.append("".join(_content))
            if key == "Range":
                _content = [f"<td style='text-align:center; padding:5px'>{key}</td>"]
                for item in value:
                    if not item:
                        _content.append("")
                        continue
                    _content.append(
                        f"<td style='text-align:center; padding:5px'>{item['Max'] - item['Min']:.4f}</td>" 
                    )
                content.append("".join(_content))
            if key == "IQR":
                _content = [f"<td style='text-align:center; padding:5px'>{key}</td>"]
                for item in value:
                    if not item:
                        _content.append("")
                        continue
                    _content.append(
                        f"<td style='text-align:center; padding:5px'>{item['Q3'] - item['Q1']:.4f}</td>" 
                    )
                content.append("".join(_content))
    output = """
        <table style="width:100%; table-layout:fixed; border-collapse:collapse; border:1px solid #ddd; margin:5px 0;">
        <colgroup>
            {colgroup}
        </colgroup>
        <thead style="background-color:#f5f5f5;">
            <tr>{header}</tr>
        </thead>
        <tbody>
            {content}
        </tbody>
        </table>
    """.format(
        colgroup="".join(
            ["<col style='width:{_width}%'>"] +
            [f'<col style="width:{width}%">' for _ in range(ld)]
        ),
        header="".join(header),
        content="".join([f"<tr>{el}</tr>" for el in content])
    )
    return "\n".join([
        line.lstrip() for line in output.splitlines()
    ]) + "\n\n"
