import logging
import textwrap
import uuid
from io import BytesIO

import requests
from PIL import Image, ImageDraw
from airflow.models import Variable

from MSI_6.utils.s3_operations import upload_file_to_s3
from MSI_6.utils.custom_exceptions import ConnectionException

logger = logging.getLogger(__name__)

IMAGE_TEXT_WIDTH = 40
TEXT_POSITION = (10, 10)
TEXT_COLOR = "black"
IMAGE_FORMAT = "JPEG"

AWS_CONN_ID = "aws_default"
S3_BUCKET = Variable.get("s3_bucket")
S3_KEY_PREFIX = Variable.get("s3_key_prefix")


def display_quote_on_image(ti):
    image_url = ti.xcom_pull(task_ids="load_image", key="image_url")
    quote = ti.xcom_pull(task_ids="load_quote", key="quote")
    author = ti.xcom_pull(task_ids="load_quote", key="author")

    response = requests.get(image_url)
    if response.status_code != 200:
        raise ConnectionException(f"Failed to download image: {response.status_code}")

    image = Image.open(BytesIO(response.content))
    draw = ImageDraw.Draw(image)

    text = f"{quote}\n- {author}"
    wrapped_text = textwrap.fill(text, width=IMAGE_TEXT_WIDTH)
    draw.multiline_text(TEXT_POSITION, wrapped_text, fill=TEXT_COLOR)

    image_bytes = BytesIO()
    image.save(image_bytes, format=IMAGE_FORMAT)
    image_bytes.seek(0)

    image_key = f"{S3_KEY_PREFIX}/random_image_{uuid.uuid4().hex}.jpg"
    upload_file_to_s3(
        aws_conn_id=AWS_CONN_ID,
        bucket_name=S3_BUCKET,
        key=image_key,
        data=image_bytes.getvalue())

    ti.xcom_push(key="image_s3_key", value=image_key)
    logger.info("Successfully uploaded image with quote to S3.")
