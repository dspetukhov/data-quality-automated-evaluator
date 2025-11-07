from typing import Sequence, Any, Dict, Union, Tuple
from plotly.subplots import make_subplots
from plotly.graph_objs import Scatter
from utility import exception_handler


@exception_handler()
def plot_data(
    x: Sequence[Any],
    *data: Sequence[Any],
    config: Dict[str, Dict[str, Any]],
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
        config (Dict[str, Dict[str, Any]]): Plotly styling settings, including:
            - 'plot': dict of Scatter style settings.
            - 'outliers': dict of outliers highlighting settings.
            - 'layout', 'grid', and other Plotly configuration options.
        file_path (str): Path to the directory where the image will be saved.
        titles (Tuple[str], optional): Titles for each subplot.

    Returns:
        List[Dict[str, float]]: List of dictionaries
            with descriptive statistics for each data series.
    """
    # Determine the number of subplots
    n_subplots = len(data)
    n_cols = 2
    # Determine the number of rows required for the given number of subplots
    n_rows = (n_subplots + n_cols - 1) // n_cols

    # Create a figure with subplots
    fig = make_subplots(
        rows=n_rows, cols=n_cols,
        horizontal_spacing=config.get("subplots", {})
        .get("horizontal_spacing", 0.1),
        vertical_spacing=config.get("subplots", {})
        .get("vertical_spacing", 0.1),
        subplot_titles=[
            el if el else "" for el in (titles or [""] * n_subplots)]
    )
    output = []
    for i in range(n_subplots):
        if data[i] is None:
            output.append({})
            continue
        # Add data series as a trace to the subplot
        fig.add_trace(
            Scatter(x=x, y=data[i], **config.get("plot", {})),
            row=(i // n_cols) + 1, col=(i % n_cols) + 1
        )
        # Evaluate data series
        ed_output, (lower_bound, upper_bound) = evaluate_data(data[i], config)
        # Highlight outliers regions using Plotly shapes
        # if lower and upper boundaries are defined
        if lower_bound and upper_bound:
            shape = (
                (data[i].min(), lower_bound),
                (upper_bound, data[i].max())
            )
            for s in range(len(shape)):
                fig.add_shape(
                    x0=min(x), x1=max(x), y0=shape[s][0], y1=shape[s][1],
                    **config.get("outliers", {}).get("style", {}),
                    row=(i // n_cols) + 1, col=(i % n_cols) + 1
                )

        output.append({
            **{"title": fig.layout.annotations[i].text},
            **ed_output})

    # Change figure size, layout, and axes
    layout = config.get("layout", {}).copy()
    height = layout.get("height", 512)
    layout.update({
        "width": height * n_cols *
        config.get("misc", {}).get("width_scale_factor", 1),
        "height": height * n_rows *
        config.get("misc", {}).get("height_scale_factor", 1)
        if n_rows > 1 else height
    })
    fig.update_layout(layout)
    fig.update_xaxes(
        tickformat="%Y-%m-%d", **config.get("grid", {}))
    fig.update_yaxes(**config.get("grid", {}))
    # Left-align for subplot titles
    for i, annotation in enumerate(fig.layout.annotations):
        annotation.update(
            x=annotation.x + (1 / n_cols) / 2.005,
            y=annotation.y + 0.005,
            xanchor="right", yanchor="bottom", font={"weight": "normal"})
    # Save figure as PNG file
    fig.write_image(
        f"{file_path}.png",
        scale=config.get("misc", {}).get("scale", 1))
    return output


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


def plot_outliers_boundaries():
    pass
