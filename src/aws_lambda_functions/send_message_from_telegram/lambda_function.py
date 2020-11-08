import os
import json
import requests


# Define global variables.
CASSANDRA_USERNAME = os.environ["CASSANDRA_USERNAME"]
CASSANDRA_PASSWORD = os.environ["CASSANDRA_PASSWORD"]
CASSANDRA_HOST = os.environ["CASSANDRA_HOST"].split(',')
CASSANDRA_PORT = int(os.environ["CASSANDRA_PORT"])
CASSANDRA_LOCAL_DC = os.environ["CASSANDRA_LOCAL_DC"]
CASSANDRA_KEYSPACE_NAME = os.environ["CASSANDRA_KEYSPACE_NAME"]
POSTGRESQL_USERNAME = os.environ["POSTGRESQL_USERNAME"]
POSTGRESQL_PASSWORD = os.environ["POSTGRESQL_PASSWORD"]
POSTGRESQL_HOST = os.environ["POSTGRESQL_HOST"]
POSTGRESQL_PORT = int(os.environ["POSTGRESQL_PORT"])
POSTGRESQL_DB_NAME = os.environ["POSTGRESQL_DB_NAME"]
TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_API_URL = "https://api.telegram.org/bot{0}/".format(TELEGRAM_BOT_TOKEN)


def lambda_handler(event, context):
    print(event['body'])

    # Initialize several variables.
    body = json.loads(event['body'])
    message = body.get("message", None)

    # Check if message is available.
    if message is not None:
        chat_id = message["chat"]["id"]
        text = message["text"]

        # Analyze incoming message from the client.
        if text == "/start":
            text = """🤖💬\nЗдравствуйте{0}! ✋\nЧем мы можем Вам помочь?""".format(
                ", {0}".format(body['message']["from"]["first_name"])
                if body['message']["from"]["first_name"] is not None
                else ""
            )
        else:
            text = "🤖💬\nВ данный момент обработка данного формата сообщения невозможна. 🤔\nПросим прощения за неудобства."

        # Send the message to the client in the bot.
        request_url = "{0}sendMessage?text={1}&chat_id={2}".format(TELEGRAM_API_URL, text, chat_id)
        requests.get(request_url)

    # Return the status code value of the request.
    return {
        "statusCode": 200
    }
