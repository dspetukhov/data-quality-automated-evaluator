
import sys
import traceback
from functools import wraps
from typing import Callable
from .setup_logging import logging


def exception_handler(exit_on_error: bool = False):
    """Decorator to handle exceptions."""
    def decorator(func: Callable) -> Callable:

        def make_message(exc_info) -> str:
            """Get necessary attributes from traceback
            and make output message."""
            exc_type, exc_obj, tb_obj = exc_info
            summary = traceback.extract_tb(tb_obj)[1]
            return "{0}: {1}#{2}: {3}: {4}".format(
                exc_type.__name__,
                summary.filename, summary.lineno, summary.line,
                str(exc_obj).lower())

        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception:
                logging.error(make_message(sys.exc_info()))
            if exit_on_error:
                sys.exit(1)
            return args[0] if args else None
        return wrapper
    return decorator
