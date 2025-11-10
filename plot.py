from typing import Sequence, Any, Dict, Union, Tuple
from plotly.subplots import make_subplots
from plotly.graph_objs import Scatter
from plotly.graph_objs._figure import Figure
from utility import exception_handler


@exception_handler()
def plot_data(
    x: Sequence[Any],
    *data: Sequence[Any],
    config: Dict[str, Any],
    file_path: str,
    titles: Tuple[str],
) -> Dict[str, float]:
    """
    Plots multiple data series as subplots using Plotly.

    Each y-series in *data is plotted in a separate subplot. The function
    supports custom plot styles, subplot arrangement, and anomaly highlighting
    (using IQR or Z-score) as specified in the `config` dictionary. The plot is
    saved to disk, and descriptive statistics for each series are returned.

    Args:
        x (Sequence[Any]): x-axis data.
        *data (Sequence[Any]): One or more y-axis data series to plot.
        config (Dict[str, Any]): Plotly styling settings, including:
            - 'plot': dict of Scatter style settings.
            - 'outliers': dict of outliers highlighting settings.
            - 'layout', 'grid', and other Plotly configuration parameters.
        file_path (str): Path to the directory where the image will be saved.
        titles (Tuple[str], optional): Titles for each subplot.

    Returns:
        List[Dict[str, float]]: List of dictionaries
            with descriptive statistics for each data series.
    """
    # Determine the number of subplots
    n_subplots = len(data)
    # Create a figure with subplots
    fig, n_cols, n_rows = create_figure(
        n_subplots,
        config.get("subplots", {}), titles
    )
    data_stats = []
    for s in range(n_subplots):
        if data[s] is None:
            data_stats.append({})
            continue
        # Add data series as a trace to the subplot
        fig.add_trace(
            Scatter(x=x, y=data[s], **config.get("plot", {})),
            row=(s // n_cols) + 1, col=(s % n_cols) + 1
        )
        # Evaluate data series
        stats, bounds = evaluate_data(data[s], config)
        # Highlight outliers regions using Plotly shapes
        fig = highlight_outliers(
            fig, s, x, data[s], bounds,
            config.get("outliers", {}).get("style", {})
        )
        data_stats.append({
            **{"title": fig.layout.annotations[s].text},
            **stats})

    # Adjust figure parameters
    fig = adjust_figure(fig, n_cols, n_rows, config)

    # Save figure as PNG file
    fig.write_image(
        f"{file_path}.png",
        scale=config.get("misc", {}).get("scale", 1))
    return data_stats


@exception_handler()
def evaluate_data(
        data: Sequence[float],
        config: Dict[str, Union[int, float]]
) -> Tuple[Dict[str, float], Tuple[float]]:
    """
    Evaluates descriptive statistic and detects outliers in data.

    This function calculates descriptive statistics and detects outliers
    in data using IQR and Z-score criteria. Configuration dictionary specifies
    IQR multiplier and Z-score threshold for outliers detection.

    Args:
        data (Sequence[float]): Sequence of data as Polars series.
        config (Dict[str, Union[int, float]]): Configuration with parameters for outlier detection:
            - 'multiplier' (float): IQR multiplier (default 1.5).
            - 'threshold' (float): Z-score threshold (default 3.0).

    Returns:
        Tuple[Dict[str, float], Tuple[float]]:
            - Dictionary with statistics:
                - Mean and standard deviation,
                - Range of values,
                - Q1 and Q3,
                - Outliers percentage according to IQR and Z-score criteria.
            - Tuple with boundaries for outliers to be highlighted on plots.

    """
    # Calculate mean and standard deviation, first and third quartile
    mean, std = data.mean(), data.std()
    q1, q3 = data.quantile(0.25), data.quantile(0.75)

    outliers_iqr, outliers_zscore, bounds = evaluate_data_outliers(
        data, mean, std, q1, q3, config
    )
    return {
        "μ±σ": (mean, std),
        "Range [Min]": data.min(),
        "Range [Max]": data.max(),
        "Range": data.max() - data.min(),
        "IQR [Q1]": q1,
        "IQR [Q3]": q3,
        "IQR": q3 - q1,
        "Anomalies [IQR]": 100 * outliers_iqr / len(data),
        "Anomalies [Z-score]": 100 * outliers_zscore / len(data),
    }, bounds


@exception_handler()
def evaluate_data_outliers(
        data: Sequence[float],
        mean: float, std: float,
        q1: float, q3: float,
        config: Dict[str, Union[int, float]]
) -> Tuple[int, Tuple[float]]:
    """
    Evaluates outliers in data.

    This function calculates the number of outliers
    according to IQR and Z-score criteria, determines boundaries
    to highlight outliers on plots if criterion was specified in configuration.

    Args:
        data (Sequence[float]): Sequence of data as Polars series.
        mean (float): Average of data.
        std (float): Standard deviation.
        q1 (float): First quartile.
        q3 (float): Third quartile.
        config (Dict[str, Union[int, float]]): Configuration with parameters for outlier detection:
            - 'multiplier' (float): IQR multiplier (default 1.5).
            - 'threshold' (float): Z-score threshold (default 3.0).

    Returns:
        Tuple[int, Tuple[float]]: Number of outliers based on IQR and Z-score,
            boundaries to highlight outliers on plots.
    """
    # Count the number of outliers based on Z-score
    if std == 0:
        outliers_zscore = 0
    else:
        outliers_zscore = (
            ((data - mean) / std).abs() > config.get("threshold", 3.0)
        ).sum()

    # Determine boundaries for outliers based on IQR
    lower_bound = q1 - config.get("multiplier", 1.5) * (q3 - q1)
    upper_bound = q3 + config.get("multiplier", 1.5) * (q3 - q1)
    # Count the number of outliers
    outliers_iqr = ((data < lower_bound) | (data > upper_bound)).sum()

    # Get boundaries to highlight outliers on plots
    # if criterion was specified in configuration
    if config.get("outliers", {}).get("criterion") == "Z-score":
        bounds = (
            mean - config.get("threshold", 3.0) * std,
            mean + config.get("threshold", 3.0) * std
        )
    elif config.get("outliers", {}).get("criterion") == "IQR":
        bounds = (lower_bound, upper_bound)
    else:
        bounds = (None, None)

    return outliers_iqr, outliers_zscore, bounds


@exception_handler()
def create_figure(
    n_subplots: int,
    config: Dict[str, Any],
    titles: Tuple[str]
) -> Figure:
    """
    Creates Plotly figure with required number of subplots.

    This function creates figure using `plotly.subplots.make_subplots`.
    The size of the subplots grid depends on the total number of subplots.
    Each subplot may have a title if the titles tuple provided.

    Args:
        n_subplots (int): Total number of subplots in the subplot grid.
        config (Dict[str, Any]): Plotly styling settings for subplots.
        titles (Tuple[str], optional): Titles for each subplot.

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
        subplot_titles=[
            el if el else "" for el in (titles or [""] * n_subplots)]
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
    Highlight outliers regions using Plotly shapes.

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
    layout = config.get("layout", {})
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
