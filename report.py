from plotly.subplots import make_subplots
from plotly.graph_objs import Scatter
from pathlib import Path
from typing import Dict, List
import polars as pl
import plotly.io as pio


pio.templates.default = "plotly_white"


def make(df: pl.LazyFrame, metadata: Dict[str, List[str]], config: Dict[Dict, str]) -> None:
    """
    Aggregate data analysis results as markdown file with plots.
    """
    output = config.get("output", "").rstrip('/')
    path = output if output else "output"
    Path(path).mkdir(exist_ok=True)
    #
    # make overview
    # fig = make_subplots(
    #     rows=1, cols=2,
    # )
    # fig.add_trace(
    #     Scatter(
    #         x=df['__date'], y=df['__count'],
    #         mode="lines+markers",
    #         line=dict(color="darkgrey", dash="dash", width=1.5),
    #         marker=dict(
    #             size=8, symbol="circle",
    #             color="white", line=dict(width=2, color="darkgrey")
    #         )
    #     ),
    #     row=1, col=1,
    # )
    # #
    # fig.update_xaxes(
    #     showgrid=True, gridcolor="lightgray", gridwidth=1, griddash="dot", title_text="Date"
    # )
    # fig.update_yaxes(
    #     showgrid=True, gridcolor="lightgray", gridwidth=1, griddash="dot", title_text="Count"
    # )
    # # Layout adjustments
    # fig.update_layout(
    #     font=dict(size=13),
    #     margin=dict(l=0, r=0, t=0, b=0)
    # )
    # fig.write_image(f"{path}/__overview.png", width=1024, height=368)
    #
    with open(f"{output}.md", "w") as file:
        file.write('# Overview\n\n')
        file.write(f"![overview]({path}/__overview.png)\n\n")
        for col in metadata:
            file.write(f"# {col}\n\n")
            file.write(f"![{col}]({path}/{col}.png)\n\n")
        # fig = make_subplots(rows=1, cols=2)
        # fig.add_trace(
        #     Scatter(
        #         x=df['__date'], y=df[col + '_' + metadata[col]["common"][0]],
        #         mode="lines+markers",
        #         line=dict(color="darkgrey", dash="dash", width=1.5),
        #         marker=dict(
        #             size=8, symbol="circle",
        #             color="white", line=dict(width=2, color="darkgrey")
        #         )
        #     ),
        #     row=1, col=1,
        # )
        # fig.add_trace(
        #     Scatter(
        #         x=df['__date'], y=df[col + '_' + metadata[col]["common"][1]],
        #         mode="lines+markers",
        #         line=dict(color="lightskyblue", dash="dash", width=1.5),
        #         marker=dict(
        #             size=8, symbol="circle",
        #             color="white", line=dict(width=2, color="darkgrey")
        #         )
        #     ),
        #     row=1, col=2,
        # )
        # #
        # fig.update_xaxes(
        #     showgrid=True, gridcolor="lightgray", gridwidth=1, griddash="dot", title_text="Date"
        # )
        # fig.update_yaxes(
        #     showgrid=True, gridcolor="lightgray", gridwidth=1, griddash="dot", title_text="Count"
        # )
        # # Layout adjustments
        # fig.update_layout(
        #     font=dict(size=13),
        #     margin=dict(l=0, r=0, t=0, b=0)
        # )
        # fig.write_image(f"{path}/{col}.png", width=1024, height=368)
        #
            if metadata[col].get("numeric"):
                file.write("## \n\n")
                file.write(f"![{col}]({path}/{col}__numeric.png)\n\n")
                # fig = make_subplots(rows=1, cols=2)
                # fig.add_trace(
                #     Scatter(
                #         x=df['__date'], y=df[col + '_' + metadata[col]["numeric"][1]],
                #         mode="lines+markers",
                #         line=dict(color="darkgrey", dash="dash", width=1.5),
                #         marker=dict(
                #             size=8, symbol="circle",
                #             color="white", line=dict(width=2, color="darkgrey")
                #         )
                #     ),
                #     row=1, col=1,
                # )
                # fig.add_trace(
                #     Scatter(
                #         x=df['__date'], y=df[col + '_' + metadata[col]["numeric"][1]],
                #         mode="lines+markers",
                #         line=dict(color="darkgrey", dash="dash", width=1.5),
                #         marker=dict(
                #             size=8, symbol="circle",
                #             color="white", line=dict(width=2, color="darkgrey")
                #         )
                #     ),
                #     row=1, col=2,
                # )
                # #
                # fig.update_xaxes(
                #     showgrid=True, gridcolor="lightgray", gridwidth=1, griddash="dot", title_text="Date"
                # )
                # fig.update_yaxes(
                #     showgrid=True, gridcolor="lightgray", gridwidth=1, griddash="dot", title_text="Count"
                # )
                # # Layout adjustments
                # fig.update_layout(
                #     font=dict(size=13),
                #     margin=dict(l=0, r=0, t=0, b=0)
                # )
                # fig.write_image(f"{path}/{col}__numeric.png", width=1024, height=368)
        # with open(f"{output}.md", "a") as file:
            # file.write('# Overview\n\n')
            # file.write(f"![overview]({path}/__overview.png)\n\n")
    return



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