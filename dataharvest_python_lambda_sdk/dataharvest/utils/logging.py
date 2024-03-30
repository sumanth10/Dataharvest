import logging

AWS_LOGGING_FORMAT = (
    "[%(levelname)s]\t%(asctime)s.%(msecs)dZ\t%(aws_request_id)s\t%(message)s\n"
)
PROCESS_LOGGING_FORMAT = "[%(process)d]\t"

# Logging function
def configure_logging_handlers(level: str):
    logger = logging.getLogger()
    for handler in logger.handlers:
        current_format: str = getattr(handler.formatter, "_fmt", AWS_LOGGING_FORMAT)
        target_format = (
            PROCESS_LOGGING_FORMAT + current_format
            if not current_format.startswith(PROCESS_LOGGING_FORMAT)
            else current_format
        )
        handler.setFormatter(logging.Formatter(target_format))
    logger.setLevel(level)
