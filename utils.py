import logging
from typing import Sequence, Any
from plotly.subplots import make_subplots
from plotly.graph_objs import Scatter
import plotly.io as pio


logging.basicConfig(level=logging.INFO)
pio.templates.default = "plotly_white"


def write_to_file(path: str, content: str) -> None:
    """Writes content to a file, handling IO errors.

    Args:
        path (str): The path to the output file.
        content (str): The content to write to the file.
    """
    try:
        with open(path, "w") as f:
            f.write(content)
    except IOError as e:
        logging.error(f"Failed to write file: {e}")


def plot_data(
    x: Sequence[Any],
    *data: Sequence[Any],
    file_path: str,
    config: dict
) -> None:
    """Plots data using Plotly.
    Each y-series in `*data` will be plotted in a separate subplot.
    Style and layout are specified by the `config` dictionary.

    Args:
        x (list): The x-axis data.
        *data (list): A collection of y-axis data.
        config (dict): Plot and layout settings.

    Returns:
        None
    """
    fig = make_subplots(rows=1, cols=len(data))
    for i in range(len(data)):
        fig.add_trace(
            Scatter(x=x, y=data[i], **config.get("plot", {})),
            row=1, col=i + 1,
        )
    fig.update_layout(**config.get("layout", {}))
    fig.update_xaxes(automargin=True)
    try:
        fig.write_image(f"{file_path}.png", width=1024, height=368)
    except ValueError as e:
        logging.error(f"Failed to represent image: {e}")
    except IOError as e:
        logging.error(f"Failed to save image: {e}")
