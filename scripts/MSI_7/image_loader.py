import logging

import requests
from airflow.providers.http.hooks.http import HttpHook

from scripts.utils.custom_exceptions import ImageLoadException

logger = logging.getLogger(__name__)


def load_image(ti):
    http = HttpHook(http_conn_id="image_api", method="GET")

    try:
        response = http.run(endpoint="")
        response.raise_for_status()
        image_url = response.url
        logger.info("Successfully loaded image URL.")
        ti.xcom_push(key="image_url", value=image_url)
    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP error: {e}")
        raise ImageLoadException(f"HTTP error: {e}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Request exception: {e}")
        raise ImageLoadException(f"Request exception: {e}")
    except Exception as e:
        logger.error(f"An unexpected error: {e}")
        raise ImageLoadException(f"An unexpected error: {e}")
