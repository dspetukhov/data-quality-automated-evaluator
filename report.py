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
    print(data)
    width = 100 // len(data)
    header, content = [], []
    for el in data:
        if not el:
            continue
        for k, v in el.items():
            if k == "title":
                header.append(
                    f"<th style='text-align:center; width:{width}%'>{v}</th>")
            if k == "Range" and isinstance(v, dict):
                el = f"{k}: {v['Max'] - v['Min']:.4f}<br/>Min: {v['Min']:.4f}<br/>Max: {v['Max']:.4f}"
            elif k == "IQR" and isinstance(v, dict):
                el = f"{k}: {v['Q3'] - v['Q1']:.4f}<br/>Q1: {v['Q1']:.4f}<br/>Q3: {v['Q3']:.4f}"
            elif k == "Anomalies" and isinstance(v, dict):
                el = f"IQR: {v['IQR']:.2f}%<br/>Z-score: {v['Z-score']:.2f}%"
            # elif isinstance(v, (int, float)):
            #     el = f"{v:.3f}"
            else:
                el = str(v)
            content.append(f"<td style='text-align:center; padding:5px'>{el}</td>")
    # if any("—" not in cell for cell in row_cells):
        # rows.append(f"<tr>{''.join(row_cells)}</tr>")

        # header.append(f"<th>{key}</th>")
        # for k, v in data[key].items():
        #     if k == "Range":
        #         v = f"{k}: {v['Max'] - v['Min']} | Min: {v['Min']} | Max: {v['Max']}"
        #     elif k == "IQR":
        #         v = f"{k}: {v['Q3'] - v['Q1']} | Q1: {v['Q1']} | Q3: {v['Q3']}"
        #     elif k == "Anomalies":
        #         v = f"{k} (%): IQR: {v['IQR']:.2f} | Z-score: {v['Z-score']:.2f}"
        #     content.append(f"<tr><td>{v}</td></tr>\n")
    output = """
        <table style="width:100%; table-layout:fixed; border-collapse:collapse; border: 1px solid #ddd; margin: 10px 0;">
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
            [f'<col style="width:{width}%">' for _ in range(len(data))]
        ),
        header="".join(header),
        content="".join(content)
    )
    return "\n".join([
        line.lstrip() for line in output.splitlines()
    ]) + "\n\n"
    # return output + "\n\n"

#     headers = [f"<th style='text-align:center; width:20%'>{key}</th>" for key in ordered_keys]
#     all_stats = set()
#     for stats_dict in data.values():
#         all_stats.update(stats_dict.keys())
#     rows = []
#     for stat_type in sorted(all_stats):
#         row_cells = []
#         for key in ordered_keys:
#             if key in data and stat_type in data[key]:
#                 value = data[key][stat_type]
#                 if stat_type == "Range" and isinstance(value, dict):
#                     cell_value = f"Range: {value['Max'] - value['Min']:.3f}<br/>Min: {value['Min']:.3f}<br/>Max: {value['Max']:.3f}"
#                 elif stat_type == "IQR" and isinstance(value, dict):
#                     cell_value = f"IQR: {value['Q3'] - value['Q1']:.3f}<br/>Q1: {value['Q1']:.3f}<br/>Q3: {value['Q3']:.3f}"
#                 elif stat_type == "Anomalies" and isinstance(value, dict):
#                     cell_value = f"IQR: {value['IQR']:.2f}%<br/>Z-score: {value['Z-score']:.2f}%"
#                 elif isinstance(value, (int, float)):
#                     cell_value = f"{value:.3f}"
#                 else:
#                     cell_value = str(value)
#             else:
#                 cell_value = "—"
#             row_cells.append(f"<td style='text-align:center; padding:5px'>{cell_value}</td>")
#         if any("—" not in cell for cell in row_cells):
#             rows.append(f"<tr>{''.join(row_cells)}</tr>")
