from typing import Sequence, Any, Dict, Tuple, List
from polars import DataFrame
from plotly.graph_objs import Scatter
from plotly.graph_objs._figure import Figure
from plotly.subplots import make_subplots
from utility import exception_handler


@exception_handler()
def plot_data(
    data: DataFrame,
    bounds: List[Tuple[float]],
    config: Dict[str, Any],
    file_path: str,
) -> Dict[str, float]:
    """
    Plots multiple data series as subplots using Plotly.

    Each y-series in *data is plotted in a separate subplot. The function
    supports custom plot styles, subplot arrangement, and anomaly highlighting
    (using IQR or Z-score) as specified in the configuration. The plot is
    saved to disk, and descriptive statistics for each series are returned.

    Args:
        data (DataFrame): Data to plot.
        bounds (List[Tuple[float]]): List of boundaries to highlight outliers.
        config (Dict[str, Any]): Plotly styling settings, including:
            - 'plot': dict of Scatter style settings.
            - 'outliers': dict of outliers highlighting settings.
            - 'layout', 'grid', and other Plotly configuration parameters.
        file_path (str): Path to the directory where the image will be saved.

    Returns:
        None
    """
    # Determine the number of subplots
    # with at least 2 subplots due to possible absence of "Target average"
    n_subplots = max(2, data.shape[1] - 1)
    # Create a figure with subplots
    fig, n_cols, n_rows = create_figure(
        n_subplots,
        config.get("subplots", {}),
        titles=data.columns[1:]
    )
    for i, col in enumerate(data.columns[1:]):
        # Add data series as a trace to the subplot
        fig.add_trace(
            Scatter(x=data["__date"], y=data[col], **config.get("plot", {})),
            row=(i // n_cols) + 1, col=(i % n_cols) + 1
        )
        # Highlight outliers regions using Plotly shapes
        fig = highlight_outliers(
            fig, i, data["__date"], data[col], bounds[i], n_cols,
            config.get("outliers", {}).get("style", {})
        )

    # Adjust figure parameters
    fig = adjust_figure(fig, n_cols, n_rows, config)

    # Save figure as PNG file
    fig.write_image(
        f"{file_path}.png",
        scale=config.get("misc", {}).get("scale", 1))


@exception_handler()
def create_figure(
    n_subplots: int,
    config: Dict[str, Any],
    titles: List[str]
) -> Figure:
    """
    Creates Plotly figure with required number of subplots.

    This function creates figure using `plotly.subplots.make_subplots`.
    The size of the subplots grid depends on the total number of subplots.
    Each subplot may have a title if the titles tuple provided.

    Args:
        n_subplots (int): Total number of subplots in the subplot grid.
        config (Dict[str, Any]): Plotly styling settings for subplots.
        titles (List[str]): Raw titles for each subplot.

    Returns:
        Tuple[Figure, int, int]: Plotly figure object,
            number of columns in subplot grid, number of rows in subplot grid.
    """
    n_cols = 2  # number of columns in the subplot grid is always equal to 2
    # Determine the number of rows required for the given number of subplots
    n_rows = (n_subplots + n_cols - 1) // n_cols
    # Create a figure with subplots
    fig = make_subplots(
        rows=n_rows, cols=n_cols,
        horizontal_spacing=config.get("horizontal_spacing", 0.1),
        vertical_spacing=config.get("vertical_spacing", 0.1),
        subplot_titles=[item.split(" __")[-1] for item in titles]
    )
    return fig, n_cols, n_rows


@exception_handler()
def highlight_outliers(
    fig: Figure,
    s: int,
    x: Sequence[Any],
    data: Sequence[Any],
    bounds: Tuple[float],
    n_cols: int,
    config: Dict[str, Any]
) -> Figure:
    """
    Highlight outliers using Plotly shapes.

    This function add shapes to highlight outliers on plots
    if boundaries were determined by `evaluate_data_outliers` function.

    Args:
        fig (Figure): Plotly figure object.
        s (int): Subplot index.
        x (Sequence[Any]): x-axis data.
        data (Sequence[Any]): y-axis data.
        bounds (Tuple[float]): Tuple with lower and upper boundaries.
        n_cols (int): Number of columns in the subplot grid.
        config (Dict[str, Any]): Plotly styling settings for outliers.

    Returns:
        Figure: Plotly figure with added shapes.
    """
    # If lower and upper boundaries are not None
    if None not in bounds:
        lower_bound, upper_bound = bounds
        shape = (
            (data.min(), lower_bound),
            (upper_bound, data.max())
        )
        for i in range(len(shape)):
            fig.add_shape(
                x0=min(x), x1=max(x), y0=shape[i][0], y1=shape[i][1],
                **config,
                row=(s // n_cols) + 1, col=(s % n_cols) + 1)
    return fig


@exception_handler()
def adjust_figure(
    fig: Figure,
    n_cols: int,
    n_rows: int,
    config: Dict[str, Any]
) -> Figure:
    """
    Adjust Plotly figure parameters.

    This function changes figure width and height based on
    the number of rows and cols and settings specified in the configuration.
    It formats tick labels for x-axis, add grid if specified in the configuration,
    align subplot titles to the left side of plots.

    Args:
        fig (Figure): Plotly figure object.
        n_cols (int): Number of columns in the subplot grid.
        n_rows (int): Number of rows in the subplot grid.
        config (Dict[str, Any]): Plotly figure configation parameters.

    Returns:
        Figure: Adjusted Plotly figure.
    """
    layout = config.get("layout", {}).copy()
    height = layout.get("height", 512)

    # Scale figure size based on the number of rows and cols
    layout.update({
        "width": height * n_cols *
        config.get("misc", {}).get("width_scale_factor", 1),
        "height": height * n_rows *
        config.get("misc", {}).get("height_scale_factor", 1)
        if n_rows > 1 else height
    })
    fig.update_layout(layout)

    # Alter x-axis tick format, add grid
    fig.update_xaxes(
        tickformat="%Y-%m-%d", **config.get("grid", {}))
    fig.update_yaxes(**config.get("grid", {}))

    # Align subplot titles to the left
    for annotation in fig.layout.annotations:
        annotation.update(
            x=annotation.x + (1 / n_cols) / 2.005,
            y=annotation.y + 0.005,
            xanchor="right", yanchor="bottom", font={"weight": "normal"})
    return fig
