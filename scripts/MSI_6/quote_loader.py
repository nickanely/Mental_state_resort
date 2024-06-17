import logging

import requests
from airflow.providers.http.hooks.http import HttpHook

from scripts.utils.custom_exceptions import QuoteLoadException

logger = logging.getLogger(__name__)


def load_quote(ti):
    http = HttpHook(http_conn_id="quote_api", method="GET")
    try:
        response = http.run(endpoint="")
        data = response.json()
        quote = data.get("content")
        author = data.get("author")
        if quote is None or author is None:
            raise Exception("Incomplete data received.")
        logger.info(f"Successfully loaded quote - {quote}")
        ti.xcom_push(key="quote", value=quote)
        ti.xcom_push(key="author", value=author)
    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP exception: {e}")
        raise QuoteLoadException(f"HTTP exception: {e}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Request exception: {e}")
        raise QuoteLoadException(f"Request exception: {e}")
