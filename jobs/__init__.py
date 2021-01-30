import logging

logging.basicConfig(
    format="%(asctime)s %(relativeCreated)6d %(threadName)s %(levelname)s: %(message)s",
    level=logging.INFO,
)


LOGGER = logging.getLogger(__name__)