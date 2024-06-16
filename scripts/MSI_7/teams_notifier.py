import logging

import requests
from airflow.models import Variable

logger = logging.getLogger(__name__)


def send_to_teams(ti):
    quote = ti.xcom_pull(key="quote")
    author = ti.xcom_pull(key="author")
    image_url = ti.xcom_pull(key="image_url")
    webhook_url = Variable.get("webhook_url")

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
                            "type": "TextBlock",
                            "text": quote,
                            "wrap": True,
                        },
                        {
                            "type": "TextBlock",
                            "text": f"-{author}",
                            "wrap": True,
                            "size": "Medium",
                            "spacing": "Medium",
                        },
                        {
                            "type": "Image",
                            "url": image_url,
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
