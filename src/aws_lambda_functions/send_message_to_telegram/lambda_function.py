import databases
import logging
import sys
import os
import json
import requests
from psycopg2.extras import RealDictCursor


"""
Define the connection to the database outside of the "lambda_handler" function.
The connection to the database will be created the first time the function is called.
Any subsequent function call will use the same database connection.
"""
postgresql_connection = None

# Define databases settings parameters.
POSTGRESQL_USERNAME = os.environ["POSTGRESQL_USERNAME"]
POSTGRESQL_PASSWORD = os.environ["POSTGRESQL_PASSWORD"]
POSTGRESQL_HOST = os.environ["POSTGRESQL_HOST"]
POSTGRESQL_PORT = int(os.environ["POSTGRESQL_PORT"])
POSTGRESQL_DB_NAME = os.environ["POSTGRESQL_DB_NAME"]
TELEGRAM_API_URL = "https://api.telegram.org"
APPSYNC_CORE_API_URL = os.environ["APPSYNC_CORE_API_URL"]
APPSYNC_CORE_API_KEY = os.environ["APPSYNC_CORE_API_KEY"]

logger = logging.getLogger(__name__)  # Create the logger with the specified name.
logger.setLevel(logging.WARNING)  # Set the logging level of the logger.


def lambda_handler(event, context):
    """
    :argument event: The AWS Lambda uses this parameter to pass in event data to the handler.
    :argument context: The AWS Lambda uses this parameter to provide runtime information to your handler.
    """
    # Parse the JSON object.
    body = json.loads(event['body'])

    # Define the values of the data passed to the function.
    chat_room_id = body["arguments"]["input"]["chatRoomId"]
    message_author_id = body["arguments"]["input"]["messageAuthorId"]
    message_channel_id = body["arguments"]["input"]["messageChannelId"]
    message_type = body["arguments"]["input"]["messageType"]
    try:
        message_text = body["arguments"]["input"]["messageText"]
    except KeyError:
        message_text = None
    try:
        message_content_url = body["arguments"]["input"]["messageContentUrl"]
    except KeyError:
        message_content_url = None
    try:
        quoted_message_id = body["arguments"]["input"]["quotedMessage"]["messageId"]
    except KeyError:
        quoted_message_id = None
    try:
        quoted_message_author_id = body["arguments"]["input"]["quotedMessage"]["messageAuthorId"]
    except KeyError:
        quoted_message_author_id = None
    try:
        quoted_message_channel_id = body["arguments"]["input"]["quotedMessage"]["messageChannelId"]
    except KeyError:
        quoted_message_channel_id = None
    try:
        quoted_message_type = body["arguments"]["input"]["quotedMessage"]["messageType"]
    except KeyError:
        quoted_message_type = None
    try:
        quoted_message_text = body["arguments"]["input"]["quotedMessage"]["messageText"]
    except KeyError:
        quoted_message_text = None
    try:
        quoted_message_content_url = body["arguments"]["input"]["quotedMessage"]["messageContentUrl"]
    except KeyError:
        quoted_message_content_url = None

    global postgresql_connection
    if not postgresql_connection:
        try:
            postgresql_connection = databases.create_postgresql_connection(
                POSTGRESQL_USERNAME,
                POSTGRESQL_PASSWORD,
                POSTGRESQL_HOST,
                POSTGRESQL_PORT,
                POSTGRESQL_DB_NAME
            )
        except Exception as error:
            logger.error(error)
            sys.exit(1)

    # With a dictionary cursor, the data is sent in a form of Python dictionaries.
    cursor = postgresql_connection.cursor(cursor_factory=RealDictCursor)

    # Prepare the SQL request that gives the minimal information about the specific chat room.
    statement = """
    select
        telegram_chat_rooms.telegram_chat_id,
        channels.channel_technical_id as telegram_bot_token
    from
        chat_rooms
    left join telegram_chat_rooms on
        chat_rooms.chat_room_id = telegram_chat_rooms.chat_room_id
    left join channels on
        chat_rooms.channel_id = channels.channel_id
    where
        chat_rooms.chat_room_id = '{0}'
    limit 1;
    """.format(chat_room_id)

    # Execute a previously prepared SQL query.
    try:
        cursor.execute(statement)
    except Exception as error:
        logger.error(error)
        sys.exit(1)

    # After the successful execution of the query commit your changes to the database.
    postgresql_connection.commit()

    # Fetch the next row of a query result set.
    aggregated_data = cursor.fetchone()
    telegram_chat_id = aggregated_data["telegram_chat_id"]
    telegram_bot_token = aggregated_data["telegram_bot_token"]

    # The cursor will be unusable from this point forward.
    cursor.close()

    # Add a new message from the client to the database.
    chat_room_message = create_chat_room_message(
        chat_room_id,
        message_author_id,
        message_channel_id,
        message_type,
        message_text
    )

    # Send a message to the Telegram chat room.
    send_message_to_telegram(telegram_bot_token, message_text, telegram_chat_id)

    # Return the object with information about created chat room message.
    return {
        "statusCode": 200,
        "body": json.dumps(chat_room_message)
    }


def send_message_to_telegram(telegram_bot_token, message_text, telegram_chat_id):
    """
    Function name:
    send_message_to_telegram

    Function description:
    The main task of this function is to send the specific message to the Telegram.
    """
    # Send a message to the Telegram chat room.
    request_url = "{0}/bot{1}/sendMessage".format(TELEGRAM_API_URL, telegram_bot_token)
    params = {
        'text': "🙂💬\n{0}".format(message_text),
        'chat_id': telegram_chat_id
    }
    try:
        response = requests.get(request_url, params=params)
        response.raise_for_status()
    except Exception as error:
        logger.error(error)
        sys.exit(1)

    # Return nothing.
    return None


def create_chat_room_message(chat_room_id, message_author_id, message_channel_id, message_type, message_text):
    """
    Function name:
    create_chat_room_message

    Function description:
    The main task of this function is to create the message in the specific chat room.
    """
    query = """
    mutation CreateChatRoomMessage (
        $chatRoomId: String!,
        $messageAuthorId: String!,
        $messageChannelId: String!,
        $messageType: String!,
        $messageText: String
    ) {
        createChatRoomMessage(
            input: {
                chatRoomId: $chatRoomId,
                messageAuthorId: $messageAuthorId,
                messageChannelId: $messageChannelId,
                messageType: $messageType,
                messageText: $messageText,
                messageContentUrl: null,
                quotedMessage: {
                    messageAuthorId: null,
                    messageChannelId: null,
                    messageContentUrl: null,
                    messageId: null,
                    messageText: null,
                    messageType: null
                }
            }
        ) {
            channelId
            chatRoomId
            messageAuthorId
            messageChannelId
            messageContentUrl
            messageCreatedDateTime
            messageDeletedDateTime
            messageId
            messageIsDelivered
            messageIsRead
            messageIsSent
            messageText
            messageType
            messageUpdatedDateTime
            quotedMessage {
                messageAuthorId
                messageChannelId
                messageContentUrl
                messageId
                messageText
                messageType
            }
        }
    }
    """
    variables = {
        "chatRoomId": chat_room_id,
        "messageAuthorId": message_author_id,
        "messageChannelId": message_channel_id,
        "messageType": message_type,
        "messageText": message_text
    }

    # Define the header setting.
    headers = {
        "x-api-key": APPSYNC_CORE_API_KEY,
        "Content-Type": "application/json"
    }

    try:
        # Make the POST request to the AppSync.
        response = requests.post(
            APPSYNC_CORE_API_URL,
            json={
                "query": query,
                "variables": variables
            },
            headers=headers
        )
        response.raise_for_status()
    except Exception as error:
        logger.error(error)
        sys.exit(1)

    # Return nothing.
    return response.json()
