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

# Define global variables.
POSTGRESQL_USERNAME = os.environ["POSTGRESQL_USERNAME"]
POSTGRESQL_PASSWORD = os.environ["POSTGRESQL_PASSWORD"]
POSTGRESQL_HOST = os.environ["POSTGRESQL_HOST"]
POSTGRESQL_PORT = int(os.environ["POSTGRESQL_PORT"])
POSTGRESQL_DB_NAME = os.environ["POSTGRESQL_DB_NAME"]
TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_API_URL = "https://api.telegram.org/bot{0}/".format(TELEGRAM_BOT_TOKEN)
APPSYNC_API_URL = os.environ["APPSYNC_API_URL"]
APPSYNC_API_KEY = os.environ["APPSYNC_API_KEY"]

logger = logging.getLogger(__name__)  # Create the logger with the specified name.
logger.setLevel(logging.WARNING)  # Set the logging level of the logger.


def lambda_handler(event, context):
    """
    :argument event: The AWS Lambda uses this parameter to pass in event data to the handler.
    :argument context: The AWS Lambda uses this parameter to provide runtime information to your handler.
    """
    # Parse the JSON object.
    body = json.loads(event['body'])
    message = body.get("message", None)

    # Check if the "message" key is available in the JSON object.
    if message is not None:
        # Define the id of the telegram chat and message text which the client sent.
        telegram_chat_id = message["chat"]["id"]
        message_text = message.get("text", None)

        # Check if message text is available.
        if message_text is not None:
            # Define client information.
            metadata = message["from"]
            first_name = metadata["first_name"]
            last_name = metadata["first_name"]
            telegram_username = metadata["username"]
            is_bot = metadata["is_bot"]

            # Check whether a person or bot writes to us.
            if is_bot is False:
                # Check the value of the message text which was sent.
                if message_text == "/start":
                    # Create a welcome message to the client who wrote to the chat bot for the first time.
                    message_text = """🤖💬\nЗдравствуйте{0}!\nЧем мы можем Вам помочь?""".format(
                        str()
                        if first_name is None
                        else ", {0}".format(first_name)
                    )
                    send_message_to_telegram(message_text, telegram_chat_id)
                else:
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

                    # Get aggregated data from the database associated with the specific chat room.
                    aggregated_data = get_chat_room_information(postgresql_connection, telegram_chat_id)

                    # Define several variables that will be used in the future.
                    if aggregated_data is not None:
                        chat_room_id = aggregated_data["chat_room_id"]
                        channel_id = aggregated_data["channel_id"]
                        chat_room_status = aggregated_data["chat_room_status"]
                        client_id = aggregated_data["client_id"]
                    else:
                        chat_room_id, channel_id, chat_room_status, client_id = None, None, None, None

                    # Check the status of the chat room.
                    if chat_room_status is None:
                        # Create new identified user in the PostgreSQL database.
                        client_id = create_identified_user(
                            postgresql_connection,
                            first_name,
                            last_name,
                            metadata,
                            telegram_username
                        )

                        # Call a mutation called "createChatRoom" from AppSync.
                        chat_room_entry = create_chat_room(TELEGRAM_BOT_TOKEN, "telegram", client_id, telegram_chat_id)

                        # Define several variables that will be used in the future.
                        chat_room_id = chat_room_entry.get("chatRoomId", None)
                        channel_id = chat_room_entry.get("channelId", None)
                    elif chat_room_status == "completed":
                        activate_closed_chat_room(chat_room_id, client_id)

                    # Add a new message from the client to the database.
                    create_chat_room_message(chat_room_id, client_id, channel_id, "text", message_text)
            else:
                text = "🤖💬\nHello my brother from another mother!"
                send_message_to_telegram(text, telegram_chat_id)
        else:
            message_text = """🤖💬\nОбработка данного формата сообщения в данный момент невозможна.\nПросим прощения 
            за доставленные временные неудобства!"""
            send_message_to_telegram(message_text, telegram_chat_id)

    # Return the status code value of the request.
    return {
        "statusCode": 200
    }


def send_message_to_telegram(message_text, telegram_chat_id):
    """
    Function name:
    send_message_to_telegram

    Function description:
    The main task of this function is to send the specific message to the Telegram.
    """
    # Send a message to the Telegram chat room.
    request_url = "{0}sendMessage".format(TELEGRAM_API_URL)
    params = {
        'text': message_text,
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


def create_chat_room(channel_technical_id, channel_type_name, client_id, telegram_chat_id):
    """
    Function name:
    create_chat_room

    Function description:
    The main task of this function is to create a chat room.
    """
    # Define the GraphQL mutation query to the AppSync.
    query = """
    mutation CreateChatRoom {{
        createChatRoom(
            input: {{
                channelTechnicalId: "{0}",
                channelTypeName: "{1}",
                clientId: "{2}",
                telegramChatId: "{3}"
            }}
        ) {{
            channel {{
                channelDescription
                channelId
                channelName
                channelTechnicalId
                channelType {{
                    channelTypeDescription
                    channelTypeId
                    channelTypeName
                }}
            }}
            channelId
            chatRoomId
            chatRoomStatus
            client {{
                userType
                userSecondaryPhoneNumber
                userSecondaryEmail
                userProfilePhotoUrl
                userPrimaryPhoneNumber
                userPrimaryEmail
                userMiddleName
                userLastName
                userId
                userFirstName
                metadata
                gender {{
                    genderId
                    genderPublicName
                    genderTechnicalName
                }}
                country {{
                    countryAlpha2Code
                    countryAlpha3Code
                    countryCodeTopLevelDomain
                    countryNumericCode
                    countryId
                    countryOfficialName
                    countryShortName
                }}
            }}
            organizationsIds
        }}
    }}
    """.format(
        channel_technical_id,
        channel_type_name,
        client_id,
        telegram_chat_id
    )

    # Define the header setting.
    headers = {
        "x-api-key": APPSYNC_API_KEY,
        "Content-Type": "application/json"
    }

    try:
        # Make the POST request to the AppSync.
        response = requests.post(
            APPSYNC_API_URL,
            json={
                "query": query
            },
            headers=headers
        )
        response.raise_for_status()
    except Exception as error:
        logger.error(error)
        sys.exit(1)

    # Return the information about the specific chat room.
    return response.json()


def create_chat_room_message(chat_room_id, message_author_id, message_channel_id, message_type, message_text):
    """
    Function name:
    create_chat_room_message

    Function description:
    The main task of this function is to create the message in the specific chat room.
    """
    query = """
    mutation CreateChatRoomMessage {{
        createChatRoomMessage(
            input: {{
                chatRoomId: "{0}",
                messageAuthorId: "{1}",
                messageChannelId: "{2}",
                messageType: "{3}",
                messageText: "{4}",
                messageContentUrl: null,
                quotedMessage: {{
                    messageAuthorId: null,
                    messageChannelId: null,
                    messageContentUrl: null,
                    messageId: null,
                    messageText: null,
                    messageType: null
                }}
            }}
        ) {{
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
            quotedMessage {{
                messageAuthorId
                messageChannelId
                messageContentUrl
                messageId
                messageText
                messageType
            }}
        }}
    }}
    """.format(
        chat_room_id,
        message_author_id,
        message_channel_id,
        message_type,
        message_text
    )

    # Define the header setting.
    headers = {
        "x-api-key": APPSYNC_API_KEY,
        "Content-Type": "application/json"
    }

    try:
        # Make the POST request to the AppSync.
        response = requests.post(
            APPSYNC_API_URL,
            json={
                "query": query
            },
            headers=headers
        )
        response.raise_for_status()
    except Exception as error:
        logger.error(error)
        sys.exit(1)

    # Return nothing.
    return None


def activate_closed_chat_room(chat_room_id, client_id):
    """
    Function name:
    activate_closed_chat_room

    Function description:
    The main task of this function is to activate a closed chat room when the client writes to it.
    """
    query = """
    mutation ActivateClosedChatRoom {{
        activateClosedChatRoom(
            input: {{
                chatRoomId: "{0}",
                clientId: "{1}"
            }}
        ) {{
            channel {{
                channelDescription
                channelId
                channelName
                channelTechnicalId
                channelType {{
                    channelTypeDescription
                    channelTypeId
                    channelTypeName
                }}
            }}
            channelId
            chatRoomId
            chatRoomStatus
            organizationsIds
            client {{
                country {{
                    countryAlpha2Code
                    countryAlpha3Code
                    countryCodeTopLevelDomain
                    countryId
                    countryNumericCode
                    countryOfficialName
                    countryShortName
                }}
                metadata
                userFirstName
                userId
                userLastName
                userMiddleName
                userPrimaryEmail
                userPrimaryPhoneNumber
                userProfilePhotoUrl
                userSecondaryEmail
                userSecondaryPhoneNumber
                userType
                gender {{
                    genderId
                    genderPublicName
                    genderTechnicalName
                }}
            }}
        }}
    }}
    """.format(
        chat_room_id,
        client_id
    )

    # Define the header setting.
    headers = {
        "x-api-key": APPSYNC_API_KEY,
        "Content-Type": "application/json"
    }

    try:
        # Make the POST request to the AppSync.
        response = requests.post(
            APPSYNC_API_URL,
            json={
                "query": query
            },
            headers=headers
        )
        response.raise_for_status()
    except Exception as error:
        logger.error(error)
        sys.exit(1)

    # Return nothing.
    return None


def get_chat_room_information(postgresql_db_connection, telegram_chat_id):
    """
    Function name:
    get_chat_room

    Function description:
    The main task of this function is to give aggregated data about the specific chat room.
    """
    # With a dictionary cursor, the data is sent in a form of Python dictionaries.
    cursor = postgresql_db_connection.cursor(cursor_factory=RealDictCursor)

    # Check if the database has chat room information for the specific telegram conversation.
    statement = """
    select
        chat_rooms.chat_room_id,
        chat_rooms.channel_id,
        chat_rooms.chat_room_status,
        (
            select
                users.user_id
            from
                chat_rooms_users_relationship
            left join users on
                chat_rooms_users_relationship.user_id = users.user_id
            where
                chat_rooms_users_relationship.chat_room_id = chat_rooms.chat_room_id
            and
                (
                    users.internal_user_id is null and users.identified_user_id is not null
                    or
                    users.internal_user_id is null and users.unidentified_user_id is not null
                )
            limit 1
        ) as client_id
    from
        chat_rooms
    left join telegram_chat_rooms on
        chat_rooms.chat_room_id = telegram_chat_rooms.chat_room_id
    where
        telegram_chat_rooms.telegram_chat_id = '{0}'
    limit 1;
    """.format(telegram_chat_id)

    # Execute a previously prepared SQL query.
    try:
        cursor.execute(statement)
    except Exception as error:
        logger.error(error)
        sys.exit(1)

    # After the successful execution of the query commit your changes to the database.
    postgresql_db_connection.commit()

    # Determine aggregated data about the specific chat room from the database.
    aggregated_data = cursor.fetchone()

    # The cursor will be unusable from this point forward.
    cursor.close()

    # Return the information about the specific chat room.
    return aggregated_data


def create_identified_user(postgresql_db_connection, first_name, last_name, metadata, telegram_username):
    """
    Function name:
    create_identified_user

    Function description:
    The main task of this function is to create a identified user in the database.
    """
    # With a dictionary cursor, the data is sent in a form of Python dictionaries.
    cursor = postgresql_db_connection.cursor(cursor_factory=RealDictCursor)

    # Prepare the SQL request that creates the new identified user.
    statement = """
    insert into identified_users(
        identified_user_first_name,
        identified_user_last_name,
        metadata,
        telegram_username
    ) values(
        '{0}',
        '{1}',
        '{2}',
        '{3}'
    )
    on conflict on constraint identified_users_telegram_username_key 
    do nothing
    returning
        identified_user_id;
    """.format(
        first_name,
        last_name,
        json.dumps(metadata),
        telegram_username
    )

    # Execute a previously prepared SQL query.
    try:
        cursor.execute(statement)
    except Exception as error:
        logger.error(error)
        sys.exit(1)

    # After the successful execution of the query commit your changes to the database.
    postgresql_db_connection.commit()

    # Define the id of the created identified user.
    identified_user_id = cursor.fetchone()["identified_user_id"]

    # Prepare the SQL request that creates the new user.
    statement = """
    insert into users(identified_user_id)
    values('{0}')
    returning
        user_id;
    """.format(identified_user_id)

    # Execute a previously prepared SQL query.
    try:
        cursor.execute(statement)
    except Exception as error:
        logger.error(error)
        sys.exit(1)

    # After the successful execution of the query commit your changes to the database.
    postgresql_db_connection.commit()

    # Define the id of the created user.
    user_id = cursor.fetchone()["user_id"]

    # Return the id of the created user.
    return user_id
