import matplotlib.pyplot as plt


def plot_time_series(df: pl.DataFrame, value_col: str, title: str):
    """
    Create time series plot using matplotlib.
    """
    fig, ax = plt.subplots(figsize=(12, 6))
    
    # Convert to pandas for plotting (if needed)
    plot_data = df.to_pandas()
    
    ax.plot(plot_data["date"], plot_data[value_col])
    ax.set_title(title)
    ax.set_xlabel("Date")
    ax.set_ylabel(value_col)
    
    plt.tight_layout()
    return fig
