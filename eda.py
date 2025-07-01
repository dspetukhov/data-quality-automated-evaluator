import polars as pl
import polars.selectors as cs
from typing import Dict, Any, Union
import logging


def make(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Perform preliminary analysis of data quality using Polars.
    """
    df = pl.read_excel(config['data_source']).lazy()
    schema = df.collect_schema()
    target_column = config.get('target_column')
    date_column = config.get('date_column')
    if not date_column:
        dc_candidates = cs.expand_selector(
            schema,
            cs.date() | cs.datetime()
        )
        if dc_candidates:
            date_column = dc_candidates[0]
        else:
            logging.error(
                'no date columns for temporal data distribution analysis'
            )
            return
    logging.info(f'base date column: {date_column}')
    aggs = []
    aggs.append(
        pl.count().alias('count'),
    )
    if target_column:
        aggs.append(
            pl.col(target_column).mean().alias('balance')
        )
    for col in schema.names():
        print(col)
        if col == date_column or col == target_column:
            continue
        aggs.extend([
            pl.col(col).n_unique().alias(f'{col}_uniq'),
            pl.col(col).is_null().mean().alias(f'{col}_null_ratio'),
        ])
    for col in cs.expand_selector(schema, cs.numeric()):
        print(col)
        aggs.extend([
            pl.col(col).min().alias(f"{col}_min"),
            pl.col(col).max().alias(f"{col}_max"),
            pl.col(col).mean().alias(f"{col}_mean"),
            pl.col(col).median().alias(f"{col}_median"),
            pl.col(col).std().alias(f"{col}_std"),
        ])
    output = {}
    # print(df.select(~(cs.numeric() | cs.duration() | cs.date() | cs.datetime())).columns)
    # Run analysis
    # aggs = []
    # output = {
        # 'general': make_general(df, date_column, target_column, agg),
    #     'detailed': make_detailed(df, date_column, target_column),
    #     'detailed_numeric': make_detailed_numeric(df, date_column, target_column)
    # }

    output = (
        df.group_by(pl.col(date_column).dt.date().alias('ymd'))
        .agg(aggs)
        .sort('ymd')
    ).collect()
    print(output)
    print(type(output))
    # Generate plots
    # plots = {
    #     "time_series": plot_time_series(
    #         results["general"],
    #         "count",
    #         "Time Series Analysis"
    #     )
    # }
    # print(output)

    return output.to_dict(as_series=True)


def make_general(
        df: pl.LazyFrame,
        date_column: str,
        target_column: Union[str, None],
        agg: list
) -> list:
    """
    Perform general analysis on the dataframe by days.
    """
    agg.append(
        pl.count().alias('total'),
    )
    if target_column:
        agg.append(
            pl.col(target_column).mean().alias('balance')
        )
    return agg


def make_detailed(
        df: pl.LazyFrame,
        date_column: str,
        target_column: Union[str, None],
        agg: list
) -> Dict[str, pl.LazyFrame]:
    """Perform analysis of categorical columns by days:
        - count of uniq values
        - ratio of null values
    """
    for col in df.select(cs.numeric()).columns:
        if col == date_column or col == target_column:
            continue
        agg.extend([
            pl.col(col).n_unique().alias(f'{col}_uniq'),
            pl.col(col).is_null().mean().alias(f'{col}_ratio')
        ])
    return agg
    # output = {}
    # # pass through each column excluding date and target columns, collect stats, and save each into dict output
    # # column['is_numeric'] = True
    # for column_name in df.columns:
    #     if column_name == date_column or column_name == target_column:
    #         continue
    #     output[column_name] = df.sql(f"""
    #         select
    #             {date_column}::date as ymd,
    #             count(distinct {column_name}) as uniq,
    #             avg({column_name} is null) as ratio
    #         from self
    #         order by ymd
    #     """)
    # return output
    #.collect_async()
    # return df.select([
    #     pl.col("category").n_unique().alias("distinct_count"),
    #     (pl.col("category").is_null().sum() / pl.len()).alias("null_ratio"),
    #     pl.col("category").dtype.alias("data_type")
    # ])


def make_detailed_numeric(
        df: pl.LazyFrame,
        date_column: str,
        target_column: Union[str, None],
        agg: list
) -> Dict[str, pl.LazyFrame]:
    """Perform analysis of numerical columns by days:
        - max value
        - median value
        - min value
        - average value
        - std_dev of value
    """
    for col in df.select(cs.numeric()).columns:
        agg.extend([
            pl.col(col).min().alias(f"{col}_min"),
            pl.col(col).max().alias(f"{col}_max"),
            pl.col(col).mean().alias(f"{col}_mean"),
            pl.col(col).median().alias(f"{col}_median"),
            pl.col(col).std().alias(f"{col}_std"),
        ])
    return agg
    # output = {}
    # for column_name in df.select(cs.numeric()).columns:
    #     if column_name == date_column or column_name == target_column:
    #         continue
    #     output[column_name] = df.sql(f"""
    #         select
    #             {date_column}::date as ymd,
    #             min({column_name}) as min,
    #             max({column_name}) as max,
    #             avg({column_name}) as average,
    #             median({column_name}) as median,
    #             stddev_samp({column_name}) as stddev
    #         from self
    #         order by ymd
    #     """)
    # return output
    # df.select(~(cs.numeric() | cs.duration() | cs.date() | cs.datetime())).columns
    # Filter numerical columns
    # numeric_cols = df.select(cs.numeric())

    # # Overall statistics
    # stats = numeric_cols.select([
    #     pl.all().min().alias("min"),
    #     pl.all().max().alias("max"),
    #     pl.all().median().alias("median"),
    #     pl.all().std().alias("stddev")
    # ])

    # df.sql("""
    #     select
    #         InvoiceDate::date as ymd,
    #         count(distinct Quantity) as uniq,
    #         avg(Quantity is null) as avg
    #     from self
    #     group by InvoiceDate::date
    #     order by ymd
    # """)
    # return output

    # Time-based statistics
    # time_stats = numeric_cols.groupby("date").agg([
    #     pl.all().min().suffix("_min"),
    #     pl.all().max().suffix("_max"),
    #     pl.all().median().suffix("_median"),
    #     pl.all().std().suffix("_std")
    # ])

    # return {"overall": stats, "time_based": time_stats}


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
