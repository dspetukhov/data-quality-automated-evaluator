from typing import Sequence, Any, Dict, List, Tuple, Union
from polars import DataFrame
from utility import exception_handler


@exception_handler()
def evaluate_data(
        data: DataFrame,
        config: Dict[str, Union[str, float]]
) -> Tuple[List[Dict[str, Any]], List[Tuple[float, float]]]:
    """
    Evaluates descriptive statistic and detects outliers in data.

    This function calculates descriptive statistics and detects outliers
    in data using IQR and Z-score criteria. Configuration dictionary specifies
    IQR multiplier and Z-score threshold for outliers detection.

    Args:
        data (DataFrame): Input data.
        config (Dict[str, Union[str, float]]): Configuration with parameters for outlier detection:
            - 'criterion' (str): Outlier detection criterion (IQR or Z-score),
            - 'multiplier' (float): IQR multiplier (default 1.5),
            - 'threshold' (float): Z-score threshold (default 3.0).

    Returns:
        Tuple[List[Dict[str, Any]], List[Tuple[float, float]]]:
            - List of dictionaries with description of each column in data:
                - name of the column,
                - mean and standard deviation,
                - range of values,
                - Q1, Q3, and IQR,
                - outliers percentage according to IQR and Z-score criteria.
            - List of boundaries for outliers to be highlighted on plots.
    """
    data_evals, outliers_bounds = [], []

    for col in data.columns[1:]:
        # Calculate mean and standard deviation, first and third quartile
        mean, std = data[col].mean(), data[col].std()
        q1, q3 = data[col].quantile(0.25), data[col].quantile(0.75)

        outliers_iqr, outliers_zscore, bounds = evaluate_data_outliers(
            data[col], mean, std, q1, q3, config
        )
        data_evals.append({
            "title": col.split(" __")[-1],
            "μ±σ": (mean, std),
            "Range [Min]": data[col].min(),
            "Range [Max]": data[col].max(),
            "Range": data[col].max() - data[col].min(),
            "IQR [Q1]": q1,
            "IQR [Q3]": q3,
            "IQR": q3 - q1,
            "Anomalies [IQR]": 100 * outliers_iqr / data.shape[0],
            "Anomalies [Z-score]": 100 * outliers_zscore / data.shape[0],
        })
        outliers_bounds.append(bounds)

    return data_evals, outliers_bounds


@exception_handler()
def evaluate_data_outliers(
        data: Sequence[float],
        mean: float, std: float,
        q1: float, q3: float,
        config: Dict[str, Union[int, float]]
) -> Tuple[int, int, Tuple[Union[float, None], Union[float, None]]]:
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
        config (Dict[str, Union[str, float]]): Parameters for outliers detection:
            - 'criterion' (str): Outlier detection criterion (IQR or Z-score),
            - 'multiplier' (float): IQR multiplier (default 1.5),
            - 'threshold' (float): Z-score threshold (default 3.0).

    Returns:
        Tuple[int, int, Tuple[Union[float, None], Union[float, None]]]:
            - number of outliers based on IQR and Z-score,
            - boundaries to highlight outliers on a plot.
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
    if config.get("criterion") == "Z-score":
        bounds = (
            mean - config.get("threshold", 3.0) * std,
            mean + config.get("threshold", 3.0) * std
        )
    elif config.get("criterion") == "IQR":
        bounds = (lower_bound, upper_bound)
    else:
        bounds = (None, None)

    return outliers_iqr, outliers_zscore, bounds
