import base64
import logging

import requests
from airflow.models import Variable

from scripts.utils.s3_operations import download_file_from_s3

logger = logging.getLogger(__name__)

AWS_CONN_ID = "aws_default"
S3_BUCKET = Variable.get("s3_bucket")


def send_to_teams(ti):
    image_s3_key = ti.xcom_pull(key="image_s3_key")
    webhook_url = Variable.get("webhook_url")

    image_data = download_file_from_s3(AWS_CONN_ID, S3_BUCKET, image_s3_key)

    image_base64 = base64.b64encode(image_data).decode("utf-8")

    message_data = {
        "type": "message",
        "attachments": [
            {
                "contentType": "application/vnd.microsoft.card.adaptive",
                "contentUrl": None,
                "content": {
                    "$schema": "https://adaptivecards.io/schemas/adaptive-card.json",
                    "type": "AdaptiveCard",
                    "version": "1.2",
                    "body": [
                        {
                            "type": "TextBlock",
                            "text": "Daily Quote - By Nikoloz Aneli",
                            "size": "Large",
                            "weight": "Bolder",
                        },
                        {
                            "type": "Image",
                            "url": f"data:image/jpeg;base64,{image_base64}",
                            "size": "Stretch",
                        },
                    ],
                },
            },
        ],
    }

    response = requests.post(webhook_url, headers={"Content-Type": "application/json"}, json=message_data)
    response.raise_for_status()
    logger.info("Message successfully sent to Teams.")
