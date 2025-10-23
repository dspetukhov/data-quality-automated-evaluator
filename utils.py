import logging
from typing import Sequence, Any, Dict
from plotly.subplots import make_subplots
from plotly.graph_objs import Scatter
import plotly.io as pio
from analysis import exception_handler

pio.templates.default = "plotly_white"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)


@exception_handler(error_message="Failed to save the plot")
def plot_data(
    x: list,
    *data: Sequence[Any],
    config: dict,
    file_path: str,
    titles: tuple,
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
        dict: A list of dictionaries with descriptive statistics
        for each data series representing columns in the aggregated LazyFrame.
    """
    n_subplots = len(data)
    n_cols = 2
    n_rows = n_cols * (n_subplots % 2) + 1
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
        fig.add_trace(
            Scatter(x=x, y=data[i], **config.get("plot", {})),
            row=(i // n_cols) + 1, col=(i % n_cols) + 1
        )
        ed_output = evaluate_data(data[i], config)
        bounds = ed_output["Anomalies"].pop("Bounds")
        if isinstance(bounds, tuple):
            lb, ub = bounds
            if lb and ub:
                shape = (
                    (data[i].min(), lb),
                    (ub, data[i].max())
                )
                for s in range(len(shape)):
                    fig.add_shape(
                        x0=min(x), x1=max(x), y0=shape[s][0], y1=shape[s][1],
                        **config.get("anomalies", {}).get("style", {}),
                        row=(i // n_cols) + 1, col=(i % n_cols) + 1
                    )

        output.append({
            **ed_output,
            **{"title": fig.layout.annotations[i].text}})

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
    # Left-align subplot titles
    for i, annotation in enumerate(fig.layout.annotations):
        annotation.update(
            x=annotation.x + (1 / n_cols) / 2.005,
            y=annotation.y + 0.005,
            xanchor="right", yanchor="bottom", font={"weight": "normal"})
    fig.write_image(
        f"{file_path}.png",
        scale=config.get("misc", {}).get("scale", 1))
    return output


def evaluate_data(
        data: Sequence[float],
        config: Dict[str, float]
) -> Dict[str, float]:
    """
    Detect anomalies in the series of data using the IQR and Z-score methods.

    Args:
        data (pl.Series): Series of data aggregated over dates.
        config (dict): Values for
            - IQR multiplier (default 1.5),
            - Z-score threshold for anomaly detection (default 3.0).

    Returns:
        dict: Dictionary with descriptive statistics including:
            - Mean and standard deviation,
            - Range of values,
            - Q1 and Q3,
            - Anomalies percent ratio according to IQR and Z-score.
    """
    # IQR
    q1 = data.quantile(0.25)
    q3 = data.quantile(0.75)
    lower_bound = q1 - config.get("multiplier", 1.5) * (q3 - q1)
    upper_bound = q3 + config.get("multiplier", 1.5) * (q3 - q1)
    anomalies_iqr = (
        (data < lower_bound) | (data > upper_bound)
    ).sum()
    # Z-score
    mean, std = data.mean(), data.std()
    if std == 0:
        anomalies_zscore = 0
    else:
        anomalies_zscore = (
            ((data - mean) / std).abs() > config.get("threshold", 3.0)
        ).sum()

    # Determine bounds for anomaly highlighting on plots
    if config.get("anomalies", {}).get("criterion") == "IQR":
        bounds = (lower_bound, upper_bound)
    elif config.get("anomalies", {}).get("criterion") == "Z-score":
        bounds = (
            mean - config.get("threshold", 3.0) * std,
            mean + config.get("threshold", 3.0) * std
        )
    else:
        bounds = None

    return {
        "μ±σ": (mean, std),
        "Range": {
            "Min": data.min(), "Max": data.max()},
        "IQR": {"Q1": q1, "Q3": q3},
        "Anomalies": {
            "IQR": 100 * anomalies_iqr / len(data),
            "Z-score": 100 * anomalies_zscore / len(data),
            "Bounds": bounds
        }
    }
