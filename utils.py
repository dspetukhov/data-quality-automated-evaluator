import logging
from plotly.subplots import make_subplots
from plotly.graph_objs import Scatter
import plotly.io as pio


logging.basicConfig(level=logging.INFO)
pio.templates.default = "plotly_white"


def plot(x, *data):
    """
    Plot data using Plotly.
    """
    # print(x, len(x))
    # print(data)
    # print(len(data), len(x))
    fig = make_subplots(rows=1, cols=len(data))
    for i in range(len(data)):
        fig.add_trace(
            Scatter(
                x=x, y=data[i],
                mode="lines+markers",
                line=dict(color="dodgerblue", dash="dash", width=.75),
                marker=dict(
                    size=7, symbol="circle",
                    color="white", line=dict(width=.5, color="dodgerblue")
                )
            ),
            row=1, col=i + 1,
        )
        fig.update_xaxes(
            showgrid=True, gridcolor="lightgray", gridwidth=1, griddash="dot", title_text="Date"
        )
        fig.update_yaxes(
            showgrid=True, gridcolor="lightgray", gridwidth=1, griddash="dot", title_text=""
        )
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
    return fig
    # 
