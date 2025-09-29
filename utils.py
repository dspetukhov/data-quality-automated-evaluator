import logging
import numpy as np
from typing import Sequence, Any, Tuple, List
from plotly.subplots import make_subplots
from plotly.graph_objs import Scatter
import plotly.io as pio
# from polars import Series


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
        rows=2 * (n_subplots % 2) + 1,
        cols=2,
        horizontal_spacing=config.get("subplots", {})
        .get("horizontal_spacing", 0.1),
        # vertical_spacing=0.05,
        subplot_titles=[
            config.get("titles", {}).get(el, el).capitalize()
            if el else "" for el in titles]
    )
    output = {}
    for i in range(n_subplots):
        if data[i] is None:
            continue
        output[fig.layout.annotations[i].text] = {
            "mean_std": f"μ±σ: {data[i].mean():.2f}±{data[i].std():.2f}",
            "Range": {
                "Min": data[i].min(),
                "Max": data[i].max()},
            "IQR": {
                "Q1": data[i].quantile(0.25),
                "Q3": data[i].quantile(0.75)},
            # TODO: calculate anomalies according to Z-score and IQR
            # "iqr_anomalies": f"Q1: {np.percentile(data[i], 25):.2f}, Q3: {np.percentile(data[i], 75):.2f}",
            # "zscore_anomalies": detect_anomalies_zscore(data[i])
        }
        fig.add_trace(
            Scatter(x=x, y=data[i], **config.get("plot", {})),
            row=i // 2 + 1, col=i % 2 + 1
        )
    width = config.get("layout", {}).get("height", 512)
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
        annotation.update(
            x=annotation.x + (1 / n_subplots) / 2.05,
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
    return output


def detect_anomalies_zscore(data: Sequence[float], threshold: float = 3.0) -> List[str]:
    """
    Detect anomalies using z-score method.

    Args:
        data (Sequence[float]): Time series data values.
        threshold (float, optional): Z-score threshold for anomaly detection. Defaults to 3.0.

    Returns:
        List[str]: Colors for each point ('green', 'yellow', 'red').
    """
    # data = np.array(data)
    print(type(data), data.shape)
    mean, std = data.mean(), data.std()
    # mean, std = np.mean(data), np.std(data)
    if std == 0:
        return ['green'] * data.shape[0]
    z_scores = np.abs((data - mean) / std)
    return [
        'green' if z < threshold * 0.5 else
        'yellow' if z < threshold else
        'red'
        for z in z_scores
    ]


# def calculate_statistics(data: Series) -> str:
#     """
#     Calculate mean and std for a series.

#     Args:
#         data (Sequence[float]): Data series.

#     Returns:
#         str: Formatted statistics string.
#     """
#     print(type(data), data.mean(), data.std())
#     mean, std = np.mean(data), np.std(data)
#     return f"μ±σ: {mean:.2f}±{std:.2f}"


# def estimate_predictive_power(
#     x: list,
#     y: list,
#     threshold: float = 3.0
# ) -> Tuple[float, List[str]]:
#     """
#     Estimate predictive power of x for y using z-score anomaly detection.

#     Args:
#         x (list): Predictor variable data.
#         y (list): Target variable data.
#         threshold (float, optional): Z-score threshold for anomaly detection. Defaults to 3.0.

#     Returns:
#         Tuple[float, List[str]]: Predictive power score and colors for each point.
#     """
#     if len(x) != len(y) or len(x) < 2:
#         return 0.0, ['green'] * len(x)
#     x_anomalies = detect_anomalies_zscore(x, threshold)
#     y_anomalies = detect_anomalies_zscore(y, threshold)
#     matches = sum(
#         1 for xa, ya in zip(x_anomalies, y_anomalies)
#         if (xa == 'red' and ya == 'red') or (xa != 'red' and ya != 'red')
#     )
#     score = matches / len(x)
#     return score, x_anomalies
