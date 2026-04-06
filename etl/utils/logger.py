import logging


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter(
        fmt="{asctime} - {levelname}:{name}: {message}",
        style="{",
        datefmt="%Y-%m-%d %H:%M"
    )

    console_handler = logging.StreamHandler()  # EMR captures stdout → S3
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger