import logging

logger = logging.getLogger(__name__)


def load_quote(ti):
    quote = "It's Wednesday, my dudes"
    author = "The Manager"
    logger.info("Successfully loaded fixed quote.")
    ti.xcom_push(key="quote", value=quote)
    ti.xcom_push(key="author", value=author)
