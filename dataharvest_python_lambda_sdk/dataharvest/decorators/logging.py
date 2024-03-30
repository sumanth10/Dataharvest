import functools
import logging

from scalable_python_lambda_sdk.scalable.utils.logging import configure_logging_handlers


# Decorator to configure logging module
def configure_logging(log_level=logging.INFO):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            configure_logging_handlers(level=log_level)
            return func(*args, **kwargs)

        return wrapper

    return decorator
