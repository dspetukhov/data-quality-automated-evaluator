from pathlib import Path
from typing import Dict, List, Union, Any
from polars import LazyFrame
from utils import plot_data, write_to_file


def make(
        df: LazyFrame,
        metadata: Dict[str, Union[List[str], str]],
        config: Dict[str, Any]
) -> None:
    """
    Aggregate data analysis results as markdown file with plots.
    """
    import os

    output = config.get("output", "output")
    Path(output).mkdir(exist_ok=True)

    with open(os.path.join(output, "README.md"), "w") as file:
        col = "__overview"
        plot_data(
            df["__date"],
            df["__count"],
            df["__balance"] if metadata.get("__balance") else None,
            file_path=f"{output}/{col}", config=config.get("plotly", {}))
        file.write("# <a name='overview'></a> Overview\n\n")
        file.write(f"![{col}]({col}.png)\n\n")

        for col in metadata:
            plot_data(
                df['__date'],
                df[col + '_' + metadata[col]["common"][0]],
                df[col + '_' + metadata[col]["common"][1]],
                file_path=f"{output}/{col}",
                config=config.get("plotly", {}))
            file.write(f"# <a name='{col}'></a> {col}\n\n")
            file.write(f"![{col}]({col}.png)\n\n")

            if metadata[col].get("numeric"):
                plot_data(
                    df['__date'],
                    df[col + '_' + metadata[col]["numeric"][0]],
                    df[col + '_' + metadata[col]["numeric"][1]],
                    df[col + '_' + metadata[col]["numeric"][2]],
                    df[col + '_' + metadata[col]["numeric"][3]],
                    df[col + '_' + metadata[col]["numeric"][4]],
                    file_path=f"{output}/{col}__numeric",
                    config=config.get("plotly", {}))
                file.write("---\n\n")
                file.write(f"![{col}]({col}__numeric.png)\n\n")
            file.write('[Back to the table of contents](#toc)\n\n')


def make_toc():
    """
    Make table of contents for the report.
    """
    output = ["Table of contents"]
    return output


# def make_categorical(db, source: str, dt: str, path: str, filter: str, helpers: dict):
#     """Get the number of uniques (`uniq_count`) and the ratio of nulls (`null_ratio`)
#     for each categorical attribute."""
#     with open(f'{path}/README.md', 'a') as file:
#         file.write('# Categorical attributes\n\n')
#         top = False
#         for key, value in helpers.items():
#             df = db.fetch(f"""
#                 select {value} as {key}
#                 from {source} {filter} limit 1500
#             """)
#             if df[key].dtype in ['float64', 'int64']:
#                 continue
#             df = db.fetch(f"""
#                 select
#                     {dt}::date as dt,
#                     count(distinct {value}) as uniq_count,
#                     sum(case when {value} is null then 1 else 0 end) / count(*) as null_ratio
#                 from {source}
#                 {filter}
#                 group by 1
#             """)
#             #
#             file.write(f"""## **{key if top else '<a name="categorical-top"></a>' + key}** - `{value}`\n\n""")
#             file.write(f'![{key}]({key}.png)\n')
#             top = True
#             #
            # _, axes = plt.subplots(nrows=1, ncols=2, figsize=(10, 4))
            # df.plot(x='dt', y='uniq_count', c='cornflowerblue', ls='--', marker='o', ms=6, mfc='white', ax=axes[0])
            # df.plot(x='dt', y='null_ratio', c='cornflowerblue', ls='--', marker='o', ms=6, mfc='white', ax=axes[1])
            # axes[0].grid(True, alpha=0.3, aa=True, ls=':', lw=1.1)
            # axes[1].grid(True, alpha=0.3, aa=True, ls=':', lw=1.1)
            # axes[0].set_title(value)
            # axes[1].set_title(value)
            # plt.tight_layout()
            # plt.savefig(f'{path}/{key}.png', dpi=100)
            # plt.close()
#             #
#             file.write('\n[Back to categorical top](#categorical-top)\n\n')


# def make_numerical(db, source: str, dt: str, path: str, filter: str, helpers: dict):
#     """Get the numerical statistics for each attribute:
#     minimum, maximum, median and standard deviation."""
#     with open(f'{path}/README.md', 'a') as file:
#         file.write('# Numerical attributes\n\n')
#         top = False
#         for key, value in helpers.items():
#             df = db.fetch(f"""
#                 select {value} as {key}
#                 from {source} {filter} limit 1500
#             """)
#             if df[key].dtype not in ['float64', 'int64']:
#                 continue
#             df = db.fetch(f"""
#                 select
#                     {dt}::date as dt,
#                     --
#                     min({value})::float as min_value,
#                     max({value})::float as max_value,
#                     -- avg({value}) as avg_value,
#                     median({value}) as median_value,
#                     stddev_samp({value}) as std_value
#                     --
#                 from {source} {filter}
#                 group by 1
#             """)
#             #
#             file.write(f"""## {key if top else '<a name="numerical-top"></a>' + key} - {value}\n\n""")
#             file.write(f'![{key}]({key}.png)\n')
#             top = True
#             #
            # _, axes = plt.subplots(nrows=1, ncols=4, figsize=(20, 4))
            # df.plot(x='dt', y='min_value', c='cornflowerblue', ls='--', marker='o', ms=6, mfc='white', label='min', ax=axes[0])
            # df.plot(x='dt', y='max_value', c='cornflowerblue', ls='--', marker='o', ms=6, mfc='white', label='max', ax=axes[1])
            # df.plot(x='dt', y='median_value', c='cornflowerblue', ls='--', marker='o', ms=6, mfc='white', label='median', ax=axes[2])
            # df.plot(x='dt', y='std_value', c='cornflowerblue', ls='--', marker='o', ms=6, mfc='white', label='stddev', ax=axes[3])
            # axes[0].grid(True, alpha=0.3, aa=True, ls=':', lw=1.1)
            # axes[1].grid(True, alpha=0.3, aa=True, ls=':', lw=1.1)
            # axes[2].grid(True, alpha=0.3, aa=True, ls=':', lw=1.1)
            # axes[3].grid(True, alpha=0.3, aa=True, ls=':', lw=1.1)
            # axes[0].set_title(value)
            # axes[1].set_title(value)
            # axes[2].set_title(value)
            # axes[3].set_title(value)
            # plt.tight_layout()
            # plt.savefig(f'{path}/{key}.png', dpi=100)
            # plt.close()
#             #
#             file.write('\n[Back to numerical top](#numerical-top)\n')

# def make_general(db, source: str, target: str, dt: str, path: str, filter: str = None):
#     """Get general information about data:
#     counts per date (`size`), class balance (`balance`)."""
#     _, axes = plt.subplots(nrows=1, ncols=2, figsize=(10, 4))
#     df.plot(x='dt', y='size',
#             c='cornflowerblue', ls='-',
#             marker='o', ms=6, mec='cornflowerblue', mfc='white', ax=axes[0])
#     df.plot(x='dt', y='balance',
#             c='cornflowerblue', ls='-',
#             marker='o', ms=6, mec='cornflowerblue', mfc='white', ax=axes[1])
#     #
#     axes[0].grid(True, alpha=0.3, aa=True, ls=':', lw=1.1)
#     axes[1].grid(True, alpha=0.3, aa=True, ls=':', lw=1.1)
#     #
#     plt.tight_layout()
#     plt.savefig(f'{path}/general.png', dpi=100)
#     plt.close()
#     with open(f'{path}/README.md', 'w') as file:
#         file.write('# General\n\n')
#         file.write('![general](general.png)\n\n')