import polars as pl
import polars.selectors as cs
from typing import Dict, Any
from utils import logging


def make_analysis(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Perform preliminary analysis using Polars.
    Perform general analysis on the dataframe by days.
    Perform analysis of categorical columns by days:
        - count of uniq values
        - ratio of null values
    Perform analysis of numerical columns by days:
        - max value
        - median value
        - min value
        - average value
        - std_dev of value
    ------------------------------------------------------------------------------
    config: {
        "source": str,  # path to the source file (Excel)
        "date_column": str | None,  # name of the date column (if None, auto-detect)
        "target_column": str | None,  # name of the target column (if any)
        "output": str | None,  # path to the output directory
    }
    ------------------------------------------------------------------------------
    returns: {
        "results": pl.DataFrame,  # dataframe with aggregated results
        "metadata": dict,  # metadata about the analysis
    }
    ------------------------------------------------------------------------------
    """
    df = pl.read_excel(config['source']).lazy()
    schema = df.collect_schema()
    target_column = config.get('target_column')
    date_column = config.get('date_column', get_date_column(schema))
    if not date_column:
        logging.error(
            "There are no date columns"
            "for temporal data distribution analysis.")
        return
    logging.info(f'base date column: {date_column}')

    aggs = [pl.count().alias("__count")]
    if target_column:
        aggs.append(
            pl.col(target_column).mean().alias("__balance")
        )
        metadata = {"__balance": True}
    else:
        metadata = {}
    for col in schema.names():
        if col in (date_column, target_column):
            continue
        aggs.extend([
            pl.col(col).n_unique().alias(f"{col}_uniq"),
            pl.col(col).is_null().mean().alias(f"{col}_null_ratio"),
        ])
        metadata[col] = {"common": ("uniq", "null_ratio")}
        if col in cs.expand_selector(schema, cs.numeric()):
            aggs.extend([
                pl.col(col).min().alias(f"{col}_min"),
                pl.col(col).max().alias(f"{col}_max"),
                pl.col(col).mean().alias(f"{col}_mean"),
                pl.col(col).median().alias(f"{col}_median"),
                pl.col(col).std().alias(f"{col}_std"),
            ])
            metadata[col]["numeric"] = ("min", "max", "mean", "median", "std")
    # Perform aggregations
    output = (
        df.group_by(pl.col(date_column).dt.date().alias('__date'))
        .agg(aggs)
        .sort('__date')
    ).collect()
    return output, metadata


def get_date_column(schema: pl.LazyFrame.schema) -> str:
    """Get the name of the date column in the dataframe."""
    candidates = cs.expand_selector(
        schema,
        cs.date() | cs.datetime()
    )
    if candidates:
        return candidates[0]


# def make_general(db, source: str, target: str, dt: str, path: str, filter: str = None):
#     """Get general information about data:
#     counts per date (`size`), class balance (`balance`)."""
#     df = db.fetch(f"""
#         select {dt}::date as dt, count(*) size, mean({target}) as balance
#         from {source}
#         {filter}
#         group by 1
#         order by 1
#     """)
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
