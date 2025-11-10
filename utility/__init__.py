from .handle_exceptions import exception_handler
from .setup_logging import logging
from .handle_data import read_source
import plotly.io as pio

pio.templates.default = "plotly_white"
