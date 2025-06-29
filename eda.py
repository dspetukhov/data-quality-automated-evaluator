import matplotlib.pyplot as plt
from pathlib import Path
import polars as pl
from typing import Dict, Any


def make(source: str, config: Dict[str, Any]) -> Dict[str, Any]:
    """Perform preliminary analysis of data quality using Polars.
    """
    df = pl.read_excel(source)  # or other appropriate reader
    print(df.head(), df.shape, df.dtypes)
    print(df.select(pl.selectors.categorical()))
    # Run analyses
    results = {
        'general': make_general(df),
        'categorical': make_categorical(df),
        'numerical': make_numerical(df)
    }

    # Generate plots
    # plots = {
    #     "time_series": plot_time_series(
    #         results["general"],
    #         "count",
    #         "Time Series Analysis"
    #     )
    # }

    return {'results': results}  #, "plots": plots}


def make_general(df: pl.DataFrame) -> pl.DataFrame:
    """Perform general analysis on the dataframe by days.
    """
    return df.sql("""
        select
            InvoiceDate::date as ymd,
            count(*) as total
        from self
        group by InvoiceDate::date
        order by ymd
    """)#.collect_async()
    # return df.group_by('InvoiceDate').agg([
    #     pl.col("value").count().alias("count"),
    #     pl.col("value").mean().alias("mean")
    # ])


def make_categorical(df: pl.DataFrame) -> pl.DataFrame:
    """Perform analysis of categorical columns by days:
        - count of uniq values
        - ratio of null values
    """
    return df.sql("""
        select
            InvoiceDate::date as ymd,
            distinct(InvoiceNo) as uniq
        from self
        group by InvoiceDate::date
        order by ymd
    """)#.collect_async()
    # return df.select([
    #     pl.col("category").n_unique().alias("distinct_count"),
    #     (pl.col("category").is_null().sum() / pl.len()).alias("null_ratio"),
    #     pl.col("category").dtype.alias("data_type")
    # ])


def make_numerical(df: pl.DataFrame) -> Dict[str, pl.DataFrame]:
    """Perform analysis of numerical columns by days:
        - max value
        - median value
        - min value
        - average value
        - std_dev of value
    """
    # Filter numerical columns
    numeric_cols = df.select(pl.col(pl.NUMERIC_DTYPES))

    # Overall statistics
    stats = numeric_cols.select([
        pl.all().min().alias("min"),
        pl.all().max().alias("max"),
        pl.all().median().alias("median"),
        pl.all().std().alias("stddev")
    ])

    # Time-based statistics
    time_stats = numeric_cols.groupby("date").agg([
        pl.all().min().suffix("_min"),
        pl.all().max().suffix("_max"),
        pl.all().median().suffix("_median"),
        pl.all().std().suffix("_std")
    ])

    return {"overall": stats, "time_based": time_stats}


# def make(
#     db, source: str,
#     target: str, dt: str,
#     path: str,
#     helpers: dict,
#     filter: str = '',
# ):
#     """Entry point into exploratory data analysis."""
#     path = path.rstrip('/')
#     Path(path).mkdir(exist_ok=True)
#     #
#     make_general(db, source, target, dt, path, filter)
#     make_categorical(db, source, dt, path, filter, helpers)
#     make_numerical(db, source, dt, path, filter, helpers)


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
#             _, axes = plt.subplots(nrows=1, ncols=2, figsize=(10, 4))
#             df.plot(x='dt', y='uniq_count', c='cornflowerblue', ls='--', marker='o', ms=6, mfc='white', ax=axes[0])
#             df.plot(x='dt', y='null_ratio', c='cornflowerblue', ls='--', marker='o', ms=6, mfc='white', ax=axes[1])
#             axes[0].grid(True, alpha=0.3, aa=True, ls=':', lw=1.1)
#             axes[1].grid(True, alpha=0.3, aa=True, ls=':', lw=1.1)
#             axes[0].set_title(value)
#             axes[1].set_title(value)
#             plt.tight_layout()
#             plt.savefig(f'{path}/{key}.png', dpi=100)
#             plt.close()
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
#             _, axes = plt.subplots(nrows=1, ncols=4, figsize=(20, 4))
#             df.plot(x='dt', y='min_value', c='cornflowerblue', ls='--', marker='o', ms=6, mfc='white', label='min', ax=axes[0])
#             df.plot(x='dt', y='max_value', c='cornflowerblue', ls='--', marker='o', ms=6, mfc='white', label='max', ax=axes[1])
#             df.plot(x='dt', y='median_value', c='cornflowerblue', ls='--', marker='o', ms=6, mfc='white', label='median', ax=axes[2])
#             df.plot(x='dt', y='std_value', c='cornflowerblue', ls='--', marker='o', ms=6, mfc='white', label='stddev', ax=axes[3])
#             axes[0].grid(True, alpha=0.3, aa=True, ls=':', lw=1.1)
#             axes[1].grid(True, alpha=0.3, aa=True, ls=':', lw=1.1)
#             axes[2].grid(True, alpha=0.3, aa=True, ls=':', lw=1.1)
#             axes[3].grid(True, alpha=0.3, aa=True, ls=':', lw=1.1)
#             axes[0].set_title(value)
#             axes[1].set_title(value)
#             axes[2].set_title(value)
#             axes[3].set_title(value)
#             plt.tight_layout()
#             plt.savefig(f'{path}/{key}.png', dpi=100)
#             plt.close()
#             #
#             file.write('\n[Back to numerical top](#numerical-top)\n')
