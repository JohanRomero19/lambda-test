import logging

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

ch = logging.StreamHandler()
ch.setLevel(logging.INFO)


formatter = logging.basicConfig(
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
)

ch.setFormatter(formatter)
LOGGER.addHandler(ch)