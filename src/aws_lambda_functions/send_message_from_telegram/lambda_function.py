import logging
import os
from psycopg2 import connect
from psycopg2.extras import RealDictCursor
from functools import wraps
from typing import *
import json
from threading import Thread
from queue import Queue
import requests
import databases

# Configure the logging tool in the AWS Lambda function.
logger = logging.getLogger(__name__)
logger.setLevel(logging.ERROR)

# Initialize constants with parameters to configure.
POSTGRESQL_USERNAME = os.environ["POSTGRESQL_USERNAME"]
POSTGRESQL_PASSWORD = os.environ["POSTGRESQL_PASSWORD"]
POSTGRESQL_HOST = os.environ["POSTGRESQL_HOST"]
POSTGRESQL_PORT = int(os.environ["POSTGRESQL_PORT"])
POSTGRESQL_DB_NAME = os.environ["POSTGRESQL_DB_NAME"]
TELEGRAM_API_URL = "https://api.telegram.org"
APPSYNC_CORE_API_URL = os.environ["APPSYNC_CORE_API_URL"]
APPSYNC_CORE_API_KEY = os.environ["APPSYNC_CORE_API_KEY"]

# The connection to the database will be created the first time the AWS Lambda function is called.
# Any subsequent call to the function will use the same database connection until the container stops.
POSTGRESQL_CONNECTION = None


def run_multithreading_tasks(functions: List[Dict[AnyStr, Union[Callable, Dict[AnyStr, Any]]]]) -> Dict[AnyStr, Any]:
    # Create the empty list to save all parallel threads.
    threads = []

    # Create the queue to store all results of functions.
    queue = Queue()

    # Create the thread for each function.
    for function in functions:
        # Check whether the input arguments have keys in their dictionaries.
        try:
            function_object = function["function_object"]
        except KeyError as error:
            logger.error(error)
            raise Exception(error)
        try:
            function_arguments = function["function_arguments"]
        except KeyError as error:
            logger.error(error)
            raise Exception(error)

        # Add the instance of the queue to the list of function arguments.
        function_arguments["queue"] = queue

        # Create the thread.
        thread = Thread(target=function_object, kwargs=function_arguments)
        threads.append(thread)

    # Start all parallel threads.
    for thread in threads:
        thread.start()

    # Wait until all parallel threads are finished.
    for thread in threads:
        thread.join()

    # Get the results of all threads.
    results = {}
    while not queue.empty():
        results = {**results, **queue.get()}

    # Return the results of all threads.
    return results


def reuse_or_recreate_postgresql_connection() -> connect:
    global POSTGRESQL_CONNECTION
    if not POSTGRESQL_CONNECTION:
        try:
            POSTGRESQL_CONNECTION = databases.create_postgresql_connection(
                POSTGRESQL_USERNAME,
                POSTGRESQL_PASSWORD,
                POSTGRESQL_HOST,
                POSTGRESQL_PORT,
                POSTGRESQL_DB_NAME
            )
        except Exception as error:
            logger.error(error)
            raise Exception("Unable to connect to the PostgreSQL database.")
    return POSTGRESQL_CONNECTION


def postgresql_wrapper(function):
    @wraps(function)
    def wrapper(**kwargs):
        try:
            postgresql_connection = kwargs["postgresql_connection"]
        except KeyError as error:
            logger.error(error)
            raise Exception(error)
        cursor = postgresql_connection.cursor(cursor_factory=RealDictCursor)
        kwargs["cursor"] = cursor
        result = function(**kwargs)
        cursor.close()
        return result
    return wrapper


@postgresql_wrapper
def get_telegram_bot_token(**kwargs) -> AnyStr:
    # Check if the input dictionary has all the necessary keys.
    try:
        cursor = kwargs["cursor"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)
    try:
        sql_arguments = kwargs["sql_arguments"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)

    # Prepare the SQL query that returns the telegram's chat bot token.
    sql_statement = """
    select
        channels.channel_technical_id as telegram_bot_token
    from
        telegram_business_accounts
    left join channels on
        telegram_business_accounts.channel_id = channels.channel_id
    where
        telegram_business_accounts.business_account = %(business_account)s
    limit 1;
    """

    # Execute the SQL query dynamically, in a convenient and safe way.
    try:
        cursor.execute(sql_statement, sql_arguments)
    except Exception as error:
        logger.error(error)
        raise Exception(error)

    # Return telegram's chat bot token.
    return cursor.fetchone()["telegram_bot_token"]


def send_message_to_telegram(**kwargs) -> None:
    # Check if the input dictionary has all the necessary keys.
    try:
        telegram_bot_token = kwargs["telegram_bot_token"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)
    try:
        message_text = kwargs["message_text"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)
    try:
        telegram_chat_id = kwargs["telegram_chat_id"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)

    # Create the request URL address.
    request_url = "{0}/bot{1}/sendMessage".format(TELEGRAM_API_URL, telegram_bot_token)

    # Create the parameters.
    parameters = {
        "text": message_text,
        "chat_id": telegram_chat_id
    }

    # Execute GET request.
    try:
        response = requests.get(request_url, params=parameters)
        response.raise_for_status()
    except Exception as error:
        logger.error(error)
        raise Exception(error)

    # Return nothing.
    return None


@postgresql_wrapper
def get_aggregated_data(**kwargs) -> Dict:
    # Check if the input dictionary has all the necessary keys.
    try:
        cursor = kwargs["cursor"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)
    try:
        sql_arguments = kwargs["sql_arguments"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)

    # Prepare the SQL query that returns the aggregated data.
    sql_statement = """
    select
        chat_rooms.chat_room_id,
        chat_rooms.channel_id,
        chat_rooms.chat_room_status,
        users.user_id as client_id
    from
        telegram_chat_rooms
    left join chat_rooms on
        telegram_chat_rooms.chat_room_id = chat_rooms.chat_room_id
    left join chat_rooms_users_relationship on
        chat_rooms.chat_room_id = chat_rooms_users_relationship.chat_room_id
    left join users on
        chat_rooms_users_relationship.user_id = users.user_id
    where
        telegram_chat_rooms.telegram_chat_id = %(telegram_chat_id)s
    and
        (
            users.internal_user_id is null and users.identified_user_id is not null
            or
            users.internal_user_id is null and users.unidentified_user_id is not null
        )
    limit 1;
    """

    # Execute the SQL query dynamically, in a convenient and safe way.
    try:
        cursor.execute(sql_statement, sql_arguments)
    except Exception as error:
        logger.error(error)
        raise Exception(error)

    # Return the aggregated data.
    return cursor.fetchone()


def create_chat_room(**kwargs) -> json:
    # Check if the input dictionary has all the necessary keys.
    try:
        channel_technical_id = kwargs["channel_technical_id"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)
    try:
        client_id = kwargs["client_id"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)
    try:
        last_message_content = kwargs["last_message_content"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)
    try:
        telegram_chat_id = kwargs["telegram_chat_id"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)

    # Define the GraphQL mutation.
    query = """
    mutation CreateChatRoom (
        $channelTechnicalId: String!,
        $clientId: String!,
        $lastMessageContent: String!,
        $telegramChatId: String
    ) {
        createChatRoom(
            input: {
                channelTechnicalId: $channelTechnicalId,
                channelTypeName: "telegram",
                clientId: $clientId,
                lastMessageContent: $lastMessageContent,
                telegramChatId: $telegramChatId
            }
        ) {
            channel {
                channelDescription
                channelId
                channelName
                channelTechnicalId
                channelType {
                    channelTypeDescription
                    channelTypeId
                    channelTypeName
                }
            }
            channelId
            chatRoomId
            chatRoomStatus
            client {
                country {
                    countryAlpha2Code
                    countryAlpha3Code
                    countryCodeTopLevelDomain
                    countryId
                    countryNumericCode
                    countryOfficialName
                    countryShortName
                }
                gender {
                    genderId
                    genderPublicName
                    genderTechnicalName
                }
                metadata
                telegramUsername
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
                whatsappProfile
                whatsappUsername
            }
            lastMessageContent
            lastMessageDateTime
            organizationsIds
            unreadMessagesNumber
        }
    }
    """

    # Define the GraphQL variables.
    variables = {
        "channelTechnicalId": channel_technical_id,
        "clientId": client_id,
        "lastMessageContent": last_message_content,
        "telegramChatId": telegram_chat_id
    }

    # Define the header setting.
    headers = {
        "x-api-key": APPSYNC_CORE_API_KEY,
        "Content-Type": "application/json"
    }

    # Execute POST request.
    try:
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
        raise Exception(error)

    # Return the JSON object of the response.
    return response.json()


@postgresql_wrapper
def create_identified_user(**kwargs) -> AnyStr:
    # Check if the input dictionary has all the necessary keys.
    try:
        cursor = kwargs["cursor"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)
    try:
        sql_arguments = kwargs["sql_arguments"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)

    # Prepare the SQL query that creates identified user.
    sql_statement = """
    insert into identified_users(
        identified_user_first_name,
        identified_user_last_name,
        metadata,
        telegram_username
    ) values(
        %(identified_user_first_name)s,
        %(identified_user_last_name)s,
        %(metadata)s,
        %(telegram_username)s
    )
    on conflict on constraint identified_users_telegram_username_key 
    do nothing
    returning
        identified_user_id::text;
    """

    # Execute the SQL query dynamically, in a convenient and safe way.
    try:
        cursor.execute(sql_statement, sql_arguments)
    except Exception as error:
        logger.error(error)
        raise Exception(error)

    # Define the id of the created identified user.
    sql_arguments["identified_user_id"] = cursor.fetchone()["identified_user_id"]

    # Prepare the SQL query that creates the user.
    sql_statement = "insert into users(identified_user_id) values(%(identified_user_id)s) returning user_id::text;"

    # Execute the SQL query dynamically, in a convenient and safe way.
    try:
        cursor.execute(sql_statement, sql_arguments)
    except Exception as error:
        logger.error(error)
        raise Exception(error)

    # Return the id of the new created user.
    return cursor.fetchone()["user_id"]


def activate_closed_chat_room(**kwargs):
    # Check if the input dictionary has all the necessary keys.
    try:
        chat_room_id = kwargs["chat_room_id"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)
    try:
        client_id = kwargs["client_id"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)
    try:
        last_message_content = kwargs["last_message_content"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)

    # Define the GraphQL mutation.
    query = """
    mutation ActivateClosedChatRoom (
        $chatRoomId: String!,
        $clientId: String!,
        $lastMessageContent: String!
    ) {
        activateClosedChatRoom(
            input: {
                chatRoomId: $chatRoomId,
                clientId: $clientId,
                lastMessageContent: $lastMessageContent
            }
        ) {
            channel {
                channelDescription
                channelId
                channelName
                channelTechnicalId
                channelType {
                    channelTypeDescription
                    channelTypeId
                    channelTypeName
                }
            }
            channelId
            chatRoomId
            chatRoomStatus
            client {
                country {
                    countryAlpha2Code
                    countryAlpha3Code
                    countryCodeTopLevelDomain
                    countryId
                    countryNumericCode
                    countryOfficialName
                    countryShortName
                }
                gender {
                    genderId
                    genderPublicName
                    genderTechnicalName
                }
                metadata
                telegramUsername
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
                whatsappProfile
                whatsappUsername
            }
            lastMessageContent
            lastMessageDateTime
            organizationsIds
            unreadMessagesNumber
        }
    }
    """

    # Define the GraphQL variables.
    variables = {
        "chatRoomId": chat_room_id,
        "clientId": client_id,
        "lastMessageContent": last_message_content
    }

    # Define the header setting.
    headers = {
        "x-api-key": APPSYNC_CORE_API_KEY,
        "Content-Type": "application/json"
    }

    # Execute POST request.
    try:
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
        raise Exception(error)

    # Return nothing.
    return None


def create_chat_room_message(**kwargs):
    # Check if the input dictionary has all the necessary keys.
    try:
        chat_room_id = kwargs["chat_room_id"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)
    try:
        message_author_id = kwargs["message_author_id"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)
    try:
        message_channel_id = kwargs["message_channel_id"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)
    try:
        message_type = kwargs["message_type"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)
    try:
        message_text = kwargs["message_text"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)

    # Define the GraphQL mutation.
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
                localMessageId: null,
                messageAuthorId: $messageAuthorId,
                messageChannelId: $messageChannelId,
                messageContentUrl: null,
                messageText: $messageText,
                messageType: $messageType,
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
            channelTypeName
            chatRoomId
            chatRoomStatus
            localMessageId
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

    # Define the GraphQL variables.
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

    # Execute POST request.
    try:
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
        raise Exception(error)

    # Return JSON object of the response.
    return response.json()


def update_message_data(**kwargs):
    # Check if the input dictionary has all the necessary keys.
    try:
        chat_room_id = kwargs["chat_room_id"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)
    try:
        messages_ids = kwargs["messages_ids"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)

    # Define the GraphQL mutation.
    query = """
    mutation UpdateMessageData (
        $chatRoomId: String!,
        $messagesIds: [String!]!
    ) {
        updateMessageData(
            input: {
                chatRoomId: $chatRoomId,
                isClient: true,
                messageStatus: MESSAGE_IS_SENT,
                messagesIds: $messagesIds
            }
        ) {
            chatRoomId
            channelId
            chatRoomMessages {
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
            chatRoomStatus
            unreadMessagesNumber,
            channelTypeName,
            isClient
        }
    }
    """

    # Define the GraphQL variables.
    variables = {
        "chatRoomId": chat_room_id,
        "messagesIds": messages_ids
    }

    # Define the header setting.
    headers = {
        "x-api-key": APPSYNC_CORE_API_KEY,
        "Content-Type": "application/json"
    }

    # Execute POST request.
    try:
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
        raise Exception(error)

    # Return nothing.
    return None


def lambda_handler(event, context):
    """
    :param event: The AWS Lambda function uses this parameter to pass in event data to the handler.
    :param context: The AWS Lambda function uses this parameter to provide runtime information to your handler.
    """
    # Parse the JSON object.
    try:
        body = json.loads(event["body"])
    except Exception as error:
        logger.error(error)
        raise Exception(error)
    message = body.get("message", None)

    # Check if the message object is available in the JSON object.
    if message:
        # Define the telegram chat id and message text which the client sent.
        try:
            telegram_chat_id = str(message["chat"]["id"])
        except Exception as error:
            logger.error(error)
            raise Exception(error)
        message_text = message.get("text", None)

        # Define the business account from which clients write.
        try:
            business_account = event['rawPath'].rsplit('/', 1)[1]
        except Exception as error:
            logger.error(error)
            raise Exception(error)

        # Define the instances of the database connections.
        postgresql_connection = reuse_or_recreate_postgresql_connection()

        # Get telegram bot token from the database.
        telegram_bot_token = get_telegram_bot_token(
            postgresql_connection=postgresql_connection,
            sql_arguments={
                "business_account": business_account
            }
        )

        # Check if message text is available.
        if message_text:
            # Define a few necessary variables that will be used in the future.
            metadata = message.get("from", None)
            first_name = metadata.get("first_name", None)
            last_name = metadata.get("last_name", None)
            telegram_username = metadata.get("username", None)
            is_bot = metadata.get("is_bot", None)

            # Check whether a person or bot writes to us.
            if not is_bot:
                # Check the value of the message text which was sent.
                if message_text == "/start":
                    # Define the message text.
                    message_text = "🤖💬\nЗдравствуйте! Чем мы можем Вам помочь?"

                    # Send the prepared text to the telegram client.
                    send_message_to_telegram(
                        telegram_bot_token=telegram_bot_token,
                        message_text=message_text,
                        telegram_chat_id=telegram_chat_id
                    )
                else:
                    # Get the aggregated data.
                    aggregated_data = get_aggregated_data(
                        postgresql_connection=postgresql_connection,
                        sql_arguments={
                            "telegram_chat_id": telegram_chat_id
                        }
                    )

                    # Define several variables that will be used in the future.
                    if aggregated_data is not None:
                        chat_room_id = aggregated_data["chat_room_id"]
                        channel_id = aggregated_data["channel_id"]
                        chat_room_status = aggregated_data["chat_room_status"]
                        client_id = aggregated_data["client_id"]
                    else:
                        chat_room_id, channel_id, chat_room_status, client_id = None, None, None, None

                    # Check the chat room status.
                    if chat_room_status is None:
                        # Create the new user.
                        client_id = create_identified_user(
                            postgresql_connection=postgresql_connection,
                            sql_arguments={
                                "identified_user_first_name": first_name,
                                "identified_user_last_name": last_name,
                                "metadata": json.dumps(metadata),
                                "telegram_username": telegram_username
                            }
                        )

                        # Create the new chat room.
                        chat_room = create_chat_room(
                            channel_technical_id=telegram_bot_token,
                            client_id=client_id,
                            last_message_content=message_text,
                            telegram_chat_id=telegram_chat_id
                        )

                        # Define a few necessary variables that will be used in the future.
                        try:
                            chat_room_id = chat_room["data"]["createChatRoom"]["chatRoomId"]
                        except Exception as error:
                            logger.error(error)
                            raise Exception(error)
                        try:
                            channel_id = chat_room["data"]["createChatRoom"]["channelId"]
                        except Exception as error:
                            logger.error(error)
                            raise Exception(error)
                    elif chat_room_status == "completed":
                        # Activate closed chat room before sending a message to the operator.
                        activate_closed_chat_room(
                            chat_room_id=chat_room_id,
                            client_id=client_id,
                            last_message_content=message_text
                        )

                    # Send the message to the operator and save it in the database.
                    chat_room_message = create_chat_room_message(
                        chat_room_id=chat_room_id,
                        message_author_id=client_id,
                        message_channel_id=channel_id,
                        message_type="text",
                        message_text=message_text
                    )

                    # Define the id of the created message.
                    try:
                        message_id = chat_room_message["data"]["createChatRoomMessage"]["messageId"]
                    except Exception as error:
                        logger.error(error)
                        raise Exception(error)

                    # Update the data (unread message number / message status) of the created message.
                    update_message_data(
                        chat_room_id=chat_room_id,
                        messages_ids=[message_id]
                    )
        else:
            # Define the message text.
            message_text = "🤖💬\nОбработка данного формата в данный момент невозможна."

            # Send the prepared text to the telegram client.
            send_message_to_telegram(
                telegram_bot_token=telegram_bot_token,
                message_text=message_text,
                telegram_chat_id=telegram_chat_id
            )

    # Return the status code 200.
    return {
        "statusCode": 200
    }
