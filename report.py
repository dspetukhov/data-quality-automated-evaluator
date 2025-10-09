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
    md_content.append(make_md_table(stats, config.get("markdown", {})))

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
        md_content.append(make_md_table(stats, config.get("markdown", {})))

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
    idx_width = config.get("index_column", 10)
    keys, data_restructured = set(), {}
    for item in data:
        if item:
            keys.update(item.keys())
    for key in keys:
        data_restructured[key] = [
            item.get(key, "") if item else ""
            for item in data
        ]

    html_style = config.get("html_style", "")
    header, col_width = make_md_table_header(
        data_restructured, html_style, idx_width)
    content = []
    for key, values in data_restructured.items():
        sample_value = next((val for val in values if val), None)
        if isinstance(sample_value, str):
            content.append(make_md_table_row(key, values, html_style))
        elif isinstance(sample_value, dict):
            content.extend(create_dict_rows(key, values, config))
        #
        # if isinstance(el, str):
        #     _content = [f"<td style='{padding};font-weight:bold'>{key}</td>"]
        #     _content.extend([
        #         f"<td style='{padding}'>{item}</td>"
        #         for item in value
        #     ])
        #     content.append("".join(_content))
        # else:
        #     for k in el:
        #         _content = [f"<td style='{padding};font-weight:bold'>{key} [{k}]</td>"]
        #         _content.extend([
        #             f"<td style='{padding}'>{v}</td>"
        #             for v in
        #             [
        #                 item.get(k) if isinstance(item, dict) else ""
        #                 for item in value
        #             ]
        #         ])
        #         content.append("".join(_content))
        #     if key == "Range":
        #         _content = [f"<td style='{padding};font-weight:bold'>{key}</td>"]
        #         for item in value:
        #             if not item:
        #                 _content.append("")
        #                 continue
        #             _content.append(
        #                 f"<td style='{padding}'>{(item['Max'] - item['Min']):.4f}</td>"
        #             )
        #         content.append("".join(_content))
        #     if key == "IQR":
        #         _content = [f"<td style='{padding};font-weight:bold'>{key}</td>"]
        #         for item in value:
        #             if not item:
        #                 _content.append("")
        #                 continue
        #             _content.append(
        #                 f"<td style='{padding}'>{(item['Q3'] - item['Q1']):.4f}</td>"
        #             )
        #         content.append("".join(_content))

    colgroup = f"<col style='width:{idx_width}%'>" + "".join([
        f'<col style="width:{col_width}%">'
        for _ in range(len(data))
    ])
    output = f"""<table style="width:100%;table-layout:fixed;border-collapse:collapse;border:1px solid #ddd;margin:5px 0;">
    <colgroup>
        {colgroup}
    </colgroup>
    <thead style="background-color:#f5f5f5;">
        <tr>{header}</tr>
    </thead>
    <tbody>
        {"".join(content)}
    </tbody>
    </table>"""
    return output + "\n\n"

    # output = """
    #     <table style="width:100%;table-layout:fixed;border-collapse:collapse;border:1px solid #ddd;margin:5px 0;">
    #     <colgroup>
    #         {colgroup}
    #     </colgroup>
    #     <thead style="background-color:#f5f5f5;">
    #         <tr>{header}</tr>
    #     </thead>
    #     <tbody>
    #         {content}
    #     </tbody>
    #     </table>
    # """.format(
    #     colgroup="".join(
    #         ["<col style='width:{idx_width}%'>"] +
    #         [f'<col style="width:{col_width}%">' for _ in range(len(header) - 1)]
    #     ),
    #     header="".join(header),
    #     content="".join([f"<tr>{el}</tr>" for el in content])
    # )
    # return "\n".join([
    #     line.lstrip() for line in output.splitlines()
    # ]) + "\n\n"


def make_md_table_header(data, style, width):
    """Create a HTML header for markdown table.

    Args:

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
    return "".join(header), col_width


def make_md_table_row(key, values, style):
    """Create HTML table row for simple string/numeric values."""
    content = [f"<td style='{style};font-weight:bold'>{key}</td>"]
    content.extend([
        f"<td style='{style}'>{val}</td>"
        for val in values
    ])
    return "<tr>" + "".join(content) + "</tr>"


def create_dict_rows(key, values, style):
    """Create HTML table rows for dictionary values (e.g., Range, IQR, Anomalies)."""
    content = []
    for value in values:
        for v in value.values():
            content.append(
                make_md_table_row(
                    0, 0, 0
                )
            )
    sample_item = next((item for item in values if item), {})
    for sub_key in sample_item.keys():
        content = [f"<td style='{style};font-weight:bold'>{key} [{sub_key}]</td>"]
        for item in values:
            if not item:
                content.append(f"<td style='{style}'></td>")
            else:
                value = item.get(sub_key, "")
                content.append(f"<td style='{style}'>{value}</td>")
        rows.append("<tr>" + "".join(content) + "</tr>")
    # Special computed rows for Range/IQR
    if key == "Range":
        content = [f"<td style='{style};font-weight:bold'>{key}</td>"]
        for item in values:
            if not item:
                content.append(f"<td style='{style}'></td>")
            else:
                range_value = item.get('Max', 0) - item.get('Min', 0)
                content.append(f"<td style='{style}'>{range_value}</td>")
        rows.append("<tr>" + "".join(content) + "</tr>")
    elif key == "IQR":
        content = [f"<td style='{style};font-weight:bold'>{key}</td>"]
        for item in values:
            if not item:
                content.append(f"<td style='{style}'></td>")
            else:
                iqr_value = item.get('Q3', 0) - item.get('Q1', 0)
                content.append(f"<td style='{style}'>{iqr_value}</td>")
        rows.append("<tr>" + "".join(content) + "</tr>")
    return rows
