import logging
from typing import Sequence, Any, Tuple
from plotly.subplots import make_subplots
from plotly.graph_objs import Scatter
import plotly.io as pio


pio.templates.default = "plotly_white"


def plot_data(
    x: list,
    *data: Sequence[Any],
    config: dict,
    file_path: str,
    titles: tuple = None,
) -> None:
    """Plots data using Plotly.
    Each y-series in `*data` will be plotted in a separate subplot.
    Style and layout are specified by the `config` dictionary.

    Args:
        x (list): The x-axis data.
        *data (list): A collection of y-axis data.
        config (dict): Plot and layout settings.
        file_path (str): Path to save the plot image (without extension).
        titles (tuple, optional): Titles for each subplot.

    Returns:
        None
    """
    n_subplots = len(data)
    fig = make_subplots(
        rows=1,
        cols=n_subplots,
        subplot_titles=[
            config.get("titles", {}).get(el, el).capitalize()
            if el else "" for el in titles]
    )
    for i in range(n_subplots):
        if data[i] is None:
            continue
        fig.add_trace(
            Scatter(x=x, y=data[i], **config.get("plot", {})),
            row=1, col=i + 1
        )
    width = config.get("layout", {}).get("height", 1024)
    width *= config.get("misc", {}).get("width_scale_factor", 1)
    width *= n_subplots
    fig.update_layout(
        **config.get("layout", {}),
        width=width)
    fig.update_xaxes(
        tickformat="%Y-%m-%d", **config.get("grid", {}))
    fig.update_yaxes(**config.get("grid", {}))
    # Left-align subplot titles
    for i, annotation in enumerate(fig.layout.annotations):
        print(annotation)
        annotation.update(
            x=annotation.x + (1 / n_subplots) / 2,
            y=annotation.y + 0.01,
            xanchor="right", yanchor="bottom", font={"weight": "normal"})
    try:
        fig.write_image(
            f"{file_path}.png",
            scale=config.get("misc", {}).get("scale", 1))
    except ValueError as e:
        logging.error(f"Failed to represent image: {e}")
    except IOError as e:
        logging.error(f"Failed to save image: {e}")
