import logging
import os
import uuid
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
FILE_STORAGE_SERVICE_URL = os.environ["FILE_STORAGE_SERVICE_URL"]

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


def check_input_arguments(**kwargs) -> None:
    # Make sure that all the necessary arguments for the AWS Lambda function are present.
    try:
        queue = kwargs["queue"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)
    try:
        input_arguments = kwargs["body"]["arguments"]["input"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)

    # Check the format and values of required arguments.
    chat_room_id = input_arguments.get("chatRoomId", None)
    if chat_room_id is not None:
        try:
            uuid.UUID(chat_room_id)
        except ValueError:
            raise Exception("The 'chatRoomId' argument format is not UUID.")
    else:
        raise Exception("The 'chatRoomId' argument can't be None/Null/Undefined.")
    message_author_id = input_arguments.get("messageAuthorId", None)
    if message_author_id is not None:
        try:
            uuid.UUID(message_author_id)
        except ValueError:
            raise Exception("The 'messageAuthorId' argument format is not UUID.")
    else:
        raise Exception("The 'messageAuthorId' argument can't be None/Null/Undefined.")
    message_channel_id = input_arguments.get("messageChannelId", None)
    if message_channel_id is not None:
        try:
            uuid.UUID(message_channel_id)
        except ValueError:
            raise Exception("The 'messageChannelId' argument format is not UUID.")
    else:
        raise Exception("The 'messageChannelId' argument can't be None/Null/Undefined.")
    message_text = input_arguments.get("messageText", None)
    message_content = input_arguments.get("messageContent", None)
    try:
        quoted_message_id = input_arguments["quotedMessage"]["messageId"]
    except KeyError:
        quoted_message_id = None
    if quoted_message_id is not None:
        try:
            uuid.UUID(quoted_message_id)
        except ValueError:
            raise Exception("The 'quotedMessageId' argument format is not UUID.")
    try:
        quoted_message_author_id = input_arguments["quotedMessage"]["messageAuthorId"]
    except KeyError:
        quoted_message_author_id = None
    if quoted_message_author_id is not None:
        try:
            uuid.UUID(quoted_message_author_id)
        except ValueError:
            raise Exception("The 'quotedMessageAuthorId' argument format is not UUID.")
    try:
        quoted_message_channel_id = input_arguments["quotedMessage"]["messageChannelId"]
    except KeyError:
        quoted_message_channel_id = None
    if quoted_message_channel_id is not None:
        try:
            uuid.UUID(quoted_message_channel_id)
        except ValueError:
            raise Exception("The 'quotedMessageChannelId' argument format is not UUID.")
    try:
        quoted_message_text = input_arguments["quotedMessage"]["messageText"]
    except KeyError:
        quoted_message_text = None
    try:
        quoted_message_content = input_arguments["quotedMessage"]["messageContent"]
    except KeyError:
        quoted_message_content = None
    local_message_id = input_arguments.get("localMessageId", None)

    # Put the result of the function in the queue.
    queue.put({
        "input_arguments": {
            "chat_room_id": chat_room_id,
            "message_author_id": message_author_id,
            "message_channel_id": message_channel_id,
            "message_text": message_text,
            "message_content": message_content,
            "quoted_message_id": quoted_message_id,
            "quoted_message_author_id": quoted_message_author_id,
            "quoted_message_channel_id": quoted_message_channel_id,
            "quoted_message_text": quoted_message_text,
            "quoted_message_content": quoted_message_content,
            "local_message_id": local_message_id
        }
    })

    # Return nothing.
    return None


def reuse_or_recreate_postgresql_connection(queue: Queue) -> None:
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
    queue.put({"postgresql_connection": POSTGRESQL_CONNECTION})
    return None


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

    # Prepare the SQL query that gives the minimal information about the chat room.
    sql_statement = """
    select
        split_part(telegram_chat_rooms.telegram_chat_id, ':', 2) as telegram_chat_id,
        channels.channel_technical_id as telegram_bot_token
    from
        chat_rooms
    left join telegram_chat_rooms on
        chat_rooms.chat_room_id = telegram_chat_rooms.chat_room_id
    left join channels on
        chat_rooms.channel_id = channels.channel_id
    where
        chat_rooms.chat_room_id = %(chat_room_id)s
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


def create_chat_room_message(**kwargs) -> Dict[AnyStr, Any]:
    # Check if the input dictionary has all the necessary keys.
    try:
        input_arguments = kwargs["input_arguments"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)
    chat_room_id = input_arguments.get("chat_room_id", None)
    message_author_id = input_arguments.get("message_author_id", None)
    message_channel_id = input_arguments.get("message_channel_id", None)
    message_text = input_arguments.get("message_text", None)
    message_content = input_arguments.get("message_content", None)
    quoted_message_id = input_arguments.get("quoted_message_id", None)
    quoted_message_author_id = input_arguments.get("quoted_message_author_id", None)
    quoted_message_channel_id = input_arguments.get("quoted_message_channel_id", None)
    quoted_message_text = input_arguments.get("quoted_message_text", None)
    quoted_message_content = input_arguments.get("quoted_message_content", None)
    local_message_id = input_arguments.get("local_message_id", None)

    # Define the GraphQL mutation.
    query = """
    mutation CreateChatRoomMessage (
        $chatRoomId: String!,
        $messageAuthorId: String!,
        $messageChannelId: String!,
        $messageText: String,
        $messageContent: String,
        $quotedMessageId: String,
        $quotedMessageAuthorId: String,
        $quotedMessageChannelId: String,
        $quotedMessageText: String,
        $quotedMessageContent: String,
        $localMessageId: String
    ) {
        createChatRoomMessage(
            input: {
                chatRoomId: $chatRoomId,
                localMessageId: $localMessageId,
                isClient: false,
                messageAuthorId: $messageAuthorId,
                messageChannelId: $messageChannelId,
                messageContent: $messageContent,
                messageText: $messageText,
                quotedMessage: {
                    messageAuthorId: $quotedMessageAuthorId,
                    messageChannelId: $quotedMessageChannelId,
                    messageContent: $quotedMessageContent,
                    messageId: $quotedMessageId,
                    messageText: $quotedMessageText
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
            messageContent
            messageCreatedDateTime
            messageDeletedDateTime
            messageId
            messageIsDelivered
            messageIsRead
            messageIsSent
            messageText
            messageUpdatedDateTime
            quotedMessage {
                messageAuthorId
                messageChannelId
                messageContent
                messageId
                messageText
            }
        }
    }
    """

    # Define the GraphQL variables.
    variables = {
        "chatRoomId": chat_room_id,
        "messageAuthorId": message_author_id,
        "messageChannelId": message_channel_id,
        "messageText": message_text,
        "messageContent": message_content,
        "quotedMessageId": quoted_message_id,
        "quotedMessageAuthorId": quoted_message_author_id,
        "quotedMessageChannelId": quoted_message_channel_id,
        "quotedMessageText": quoted_message_text,
        "quotedMessageContent": quoted_message_content,
        "localMessageId": local_message_id
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


def send_message_text_to_telegram(**kwargs) -> None:
    # Check if the input dictionary has all the necessary keys.
    try:
        telegram_bot_token = kwargs["telegram_bot_token"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)
    try:
        telegram_chat_id = kwargs["telegram_chat_id"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)
    try:
        message_text = kwargs["message_text"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)

    # Create the request URL address.
    request_url = "{0}/bot{1}/sendMessage".format(TELEGRAM_API_URL, telegram_bot_token)

    # Create the parameters.
    parameters = {
        "chat_id": telegram_chat_id,
        "text": message_text
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


def get_the_presigned_url(**kwargs) -> AnyStr:
    # Check if the input dictionary has all the necessary keys.
    try:
        file_url = kwargs["file_url"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)

    # Create the request URL address.
    request_url = "{0}/get_presigned_url_to_download_file".format(FILE_STORAGE_SERVICE_URL)

    # Create the parameters.
    parameters = {
        "key": file_url.split('/', 3)[-1]
    }

    # Execute GET request.
    try:
        response = requests.get(request_url, params=parameters)
        response.raise_for_status()
    except Exception as error:
        logger.error(error)
        raise Exception(error)

    # Define the value of the presigned url of the document.
    try:
        presigned_url = response.json()["data"]
    except Exception as error:
        logger.error(error)
        raise Exception(error)

    # Return the value of the presigned url.
    return presigned_url


def send_gif_to_telegram(**kwargs) -> None:
    # Check if the input dictionary has all the necessary keys.
    try:
        telegram_bot_token = kwargs["telegram_bot_token"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)
    try:
        telegram_chat_id = kwargs["telegram_chat_id"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)
    try:
        gif_url = kwargs["gif_url"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)

    # Create the request URL address.
    # https://core.telegram.org/bots/api#sendanimation
    request_url = "{0}/bot{1}/sendAnimation".format(TELEGRAM_API_URL, telegram_bot_token)

    # Create the parameters.
    parameters = {
        "chat_id": telegram_chat_id,
        "animation": gif_url
    }

    # Execute the POST request.
    try:
        response = requests.post(request_url, params=parameters)
        response.raise_for_status()
    except Exception as error:
        logger.error(error)
        raise Exception(error)

    # Return nothing.
    return None


def send_document_to_telegram(**kwargs) -> None:
    # Check if the input dictionary has all the necessary keys.
    try:
        telegram_bot_token = kwargs["telegram_bot_token"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)
    try:
        telegram_chat_id = kwargs["telegram_chat_id"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)
    try:
        document_url = kwargs["document_url"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)
    try:
        caption = kwargs["caption"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)

    # Create the request URL address.
    # https://core.telegram.org/bots/api#senddocument
    request_url = "{0}/bot{1}/sendDocument".format(TELEGRAM_API_URL, telegram_bot_token)

    # Create the parameters.
    parameters = {
        "chat_id": telegram_chat_id,
        "document": document_url
    }

    # Document caption (may also be used when resending documents by file_id), 0-1024 characters after entities parsing.
    if caption is not None:
        parameters["caption"] = caption

    # Execute the POST request.
    try:
        response = requests.post(request_url, params=parameters)
        response.raise_for_status()
    except Exception as error:
        logger.error(error)
        raise Exception(error)

    # Return nothing.
    return None


def send_image_to_telegram(**kwargs) -> None:
    # Check if the input dictionary has all the necessary keys.
    try:
        telegram_bot_token = kwargs["telegram_bot_token"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)
    try:
        telegram_chat_id = kwargs["telegram_chat_id"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)
    try:
        image_url = kwargs["image_url"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)
    try:
        caption = kwargs["caption"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)

    # Create the request URL address.
    # https://core.telegram.org/bots/api#sendphoto
    request_url = "{0}/bot{1}/sendPhoto".format(TELEGRAM_API_URL, telegram_bot_token)

    # Create the parameters.
    parameters = {
        "chat_id": telegram_chat_id,
        "photo": image_url
    }

    # Photo caption (may also be used when resending photos by file_id), 0-1024 characters after entities parsing.
    if caption is not None:
        parameters["caption"] = caption

    # Execute the POST request.
    try:
        response = requests.post(request_url, params=parameters)
        response.raise_for_status()
    except Exception as error:
        logger.error(error)
        raise Exception(error)

    # Return nothing.
    return None


def send_video_to_telegram(**kwargs) -> None:
    # Check if the input dictionary has all the necessary keys.
    try:
        telegram_bot_token = kwargs["telegram_bot_token"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)
    try:
        telegram_chat_id = kwargs["telegram_chat_id"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)
    try:
        video_url = kwargs["video_url"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)
    try:
        caption = kwargs["caption"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)

    # Create the request URL address.
    # https://core.telegram.org/bots/api#sendvideo
    request_url = "{0}/bot{1}/sendVideo".format(TELEGRAM_API_URL, telegram_bot_token)

    # Create the parameters.
    parameters = {
        "chat_id": telegram_chat_id,
        "video": video_url
    }

    # Video caption (may also be used when resending videos by file_id), 0-1024 characters after entities parsing.
    if caption is not None:
        parameters["caption"] = caption

    # Execute the POST request.
    try:
        response = requests.post(request_url, params=parameters)
        response.raise_for_status()
    except Exception as error:
        logger.error(error)
        raise Exception(error)

    # Return nothing.
    return None


def send_audio_to_telegram(**kwargs) -> None:
    # Check if the input dictionary has all the necessary keys.
    try:
        telegram_bot_token = kwargs["telegram_bot_token"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)
    try:
        telegram_chat_id = kwargs["telegram_chat_id"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)
    try:
        audio_url = kwargs["audio_url"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)
    try:
        caption = kwargs["caption"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)

    # Create the request URL address.
    # https://core.telegram.org/bots/api#sendaudio
    request_url = "{0}/bot{1}/sendAudio".format(TELEGRAM_API_URL, telegram_bot_token)

    # Create the parameters.
    parameters = {
        "chat_id": telegram_chat_id,
        "audio": audio_url
    }

    # Audio caption, 0-1024 characters after entities parsing.
    if caption is not None:
        parameters["caption"] = caption

    # Execute the POST request.
    try:
        response = requests.post(request_url, params=parameters)
        response.raise_for_status()
    except Exception as error:
        logger.error(error)
        raise Exception(error)

    # Return nothing.
    return None


def send_collection_to_telegram(**kwargs) -> None:
    # Check if the input dictionary has all the necessary keys.
    try:
        telegram_bot_token = kwargs["telegram_bot_token"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)
    try:
        telegram_chat_id = kwargs["telegram_chat_id"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)
    try:
        collection = kwargs["collection"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)

    # Create the request URL address.
    # https://core.telegram.org/bots/api#sendmediagroup
    request_url = "{0}/bot{1}/sendMediaGroup".format(TELEGRAM_API_URL, telegram_bot_token)

    # Define the JSON object body of the POST request.
    data = {
        "chat_id": telegram_chat_id,
        "media": collection
    }

    # Execute the POST request.
    try:
        response = requests.post(request_url, data=json.dumps(data))
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

    # Run several initialization functions in parallel.
    results_of_tasks = run_multithreading_tasks([
        {
            "function_object": check_input_arguments,
            "function_arguments": {
                "body": body
            }
        },
        {
            "function_object": reuse_or_recreate_postgresql_connection,
            "function_arguments": {}
        }
    ])

    # Define the instances of the database connections.
    postgresql_connection = results_of_tasks["postgresql_connection"]

    # Define the input arguments of the AWS Lambda function.
    input_arguments = results_of_tasks["input_arguments"]
    chat_room_id = input_arguments.get("chat_room_id", None)
    message_text = input_arguments.get("message_text", None)
    message_content = input_arguments.get("message_content", None)

    # Get the aggregated data.
    aggregated_data = get_aggregated_data(
        postgresql_connection=postgresql_connection,
        sql_arguments={
            "chat_room_id": chat_room_id
        }
    )

    # Define a few necessary variables that will be used in the future.
    try:
        telegram_chat_id = aggregated_data["telegram_chat_id"]
    except Exception as error:
        logger.error(error)
        raise Exception(error)
    try:
        telegram_bot_token = aggregated_data["telegram_bot_token"]
    except Exception as error:
        logger.error(error)
        raise Exception(error)

    # Send the message to the operator and save it in the database.
    chat_room_message = create_chat_room_message(input_arguments=input_arguments)

    # Send the message text to the telegram.
    if message_text is not None and message_content is None:
        send_message_text_to_telegram(
            telegram_bot_token=telegram_bot_token,
            telegram_chat_id=telegram_chat_id,
            message_text=message_text
        )

    # Check the value of the message content.
    if message_content is not None:
        # Define the list of files.
        files = json.loads(message_content)

        # Define the number of files.
        files_count = len(files)

        # Depending on the number of files, we use different methods of the telegram api for correct visualization.
        if files_count == 1:
            # Define the file object.
            file = files[0]

            # Define the category of the file.
            file_category = file["category"]

            # Defile the url address of the file.
            file_url = file["url"]

            # Check file's category and send it to the telegram with the correct telegram api method.
            if file_category == "gif":
                # Send the gif to the telegram.
                send_gif_to_telegram(
                    telegram_bot_token=telegram_bot_token,
                    telegram_chat_id=telegram_chat_id,
                    gif_url=file_url
                )
            elif file_category == "document":
                # Send the document to the telegram.
                send_document_to_telegram(
                    telegram_bot_token=telegram_bot_token,
                    telegram_chat_id=telegram_chat_id,
                    document_url=get_the_presigned_url(file_url=file_url),
                    caption=message_text
                )
            elif file_category == "image":
                # Send the image to the telegram.
                send_image_to_telegram(
                    telegram_bot_token=telegram_bot_token,
                    telegram_chat_id=telegram_chat_id,
                    image_url=get_the_presigned_url(file_url=file_url),
                    caption=message_text
                )
            elif file_category == "video":
                # Send the video to the telegram.
                send_video_to_telegram(
                    telegram_bot_token=telegram_bot_token,
                    telegram_chat_id=telegram_chat_id,
                    video_url=get_the_presigned_url(file_url=file_url),
                    caption=message_text
                )
            elif file_category == "audio":
                # Send the audio to the telegram.
                send_audio_to_telegram(
                    telegram_bot_token=telegram_bot_token,
                    telegram_chat_id=telegram_chat_id,
                    audio_url=get_the_presigned_url(file_url=file_url),
                    caption=message_text
                )
            else:
                pass
        elif 1 < files_count <= 10:
            # Define the empty list of collection.
            collection = []

            # Generate the correct collection format.
            for file in files:
                # Define the value of the presigned url.
                presigned_url = get_the_presigned_url(file_url=file["url"])

                # Add the new item to the list of collection.
                if presigned_url is not None:
                    media = {
                        "type": "document",
                        "media": presigned_url
                    }
                    if message_text is not None:
                        media["caption"] = message_text
                    collection.append(media)

            # Send the collection to the telegram.
            send_collection_to_telegram(
                telegram_bot_token=telegram_bot_token,
                telegram_chat_id=telegram_chat_id,
                collection=collection
            )
        else:
            pass

    # Return the status code 200.
    return {
        "statusCode": 200,
        "body": json.dumps(chat_room_message)
    }
