import databases
import logging
import sys
import os
import json
import requests
import uuid
from cassandra.query import SimpleStatement
from cassandra import ConsistencyLevel
from psycopg2.extras import RealDictCursor

"""
Define connections to databases outside of the "lambda_handler" function.
Connections to databases will be created the first time the function is called.
Any subsequent function call will use the same database connections.
"""
cassandra_connection = None
postgresql_connection = None

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
                    # Since connections with databases were defined outside of the function, we create global variables.
                    global cassandra_connection
                    if not cassandra_connection:
                        try:
                            cassandra_connection = databases.create_cassandra_connection(
                                CASSANDRA_USERNAME,
                                CASSANDRA_PASSWORD,
                                CASSANDRA_HOST,
                                CASSANDRA_PORT,
                                CASSANDRA_LOCAL_DC
                            )
                        except Exception as error:
                            logger.error(error)
                            sys.exit(1)
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

                    # Set the name of the keyspace you will be working with.
                    # This statement fix ERROR NoHostAvailable: ('Unable to complete the operation against any hosts').
                    success = False
                    while not success:
                        try:
                            cassandra_connection.set_keyspace(CASSANDRA_KEYSPACE_NAME)
                            success = True
                        except Exception as error:
                            try:
                                cassandra_connection = databases.create_cassandra_connection(
                                    CASSANDRA_USERNAME,
                                    CASSANDRA_PASSWORD,
                                    CASSANDRA_HOST,
                                    CASSANDRA_PORT,
                                    CASSANDRA_LOCAL_DC
                                )
                            except Exception as error:
                                logger.error(error)
                                sys.exit(1)

                    # Get information about the specific chat room from the PostgreSQL database.
                    chat_room_entry = get_chat_room(postgresql_connection, telegram_chat_id)

                    # Define several variables that will be used in the future.
                    if chat_room_entry is None:
                        chat_room_id = None
                        channel_id = None
                        chat_room_status = None
                    else:
                        chat_room_id = chat_room_entry["chat_room_id"]
                        channel_id = chat_room_entry["channel_id"]
                        chat_room_status = chat_room_entry["chat_room_status"]

                    # Define the variable with the ID of the client.
                    client_id = get_client(postgresql_connection, telegram_username)

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

                        # Create chat room in the PostgreSQL database.
                        chat_room_entry = create_chat_room(
                            postgresql_connection,
                            cassandra_connection,
                            client_id,
                            telegram_chat_id
                        )

                        # Define several variables that will be used in the future.
                        chat_room_id = chat_room_entry.get("chat_room_id", None)
                        channel_id = chat_room_entry.get("channel_id", None)

                        # Record in the database the client who writes to the telegram chat room as a member
                        add_chat_room_member(
                            postgresql_connection,
                            chat_room_id,
                            client_id
                        )
                    elif chat_room_status == "accepted":
                        # Update the last message of a specific chat room.
                        update_chat_room_last_message(
                            postgresql_connection,
                            cassandra_connection,
                            message_text,
                            channel_id,
                            chat_room_id
                        )
                    elif chat_room_status == "completed":
                        # Open a previously completed dialog.
                        activate_closed_chat_room(
                            postgresql_connection,
                            cassandra_connection,
                            chat_room_id,
                            channel_id,
                            client_id
                        )

                    # Add a new message from the client to the Cassandra database.
                    create_chat_room_message(
                        cassandra_connection,
                        chat_room_id,
                        client_id,
                        channel_id,
                        message_text
                    )
            else:
                text = "🤖💬\nHello my brother from another mother!"
                send_message_to_telegram(text, telegram_chat_id)
        else:
            message_text = """🤖💬\nОбработка данного формата сообщения в данный момент невозможна.\nПросим прощения 
            за доставленные временные неудобства! """
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
    The main task of this function is to send a message to Telegram.
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
    return None


def get_chat_room(postgresql_db_connection, telegram_chat_id):
    """
    Function name:
    get_chat_room

    Function description:
    The main task of this function is to give information about a specific chat room.
    """
    # With a dictionary cursor, the data is sent in a form of Python dictionaries.
    cursor = postgresql_db_connection.cursor(cursor_factory=RealDictCursor)

    # Check if the database has chat room information for the specific telegram conversation.
    statement = """
    select
        chat_rooms.chat_room_id,
        chat_rooms.channel_id,
        chat_rooms.chat_room_status
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

    # Determine information about the chat room from the database.
    chat_room_entry = cursor.fetchone()

    # The cursor will be unusable from this point forward.
    cursor.close()

    # Return the information about the specific chat room.
    return chat_room_entry


def create_chat_room(postgresql_db_connection, cassandra_db_connection, client_id, telegram_chat_id):
    """
    Function name:
    create_chat_room

    Function description:
    The main task of this function is to create a chat room in the database.
    """
    # With a dictionary cursor, the data is sent in a form of Python dictionaries.
    cursor = postgresql_db_connection.cursor(cursor_factory=RealDictCursor)

    # Generate a unique ID for a new non accepted chat room.
    chat_room_id = str(uuid.uuid1())

    # Create the chat room for the specific telegram conversation.
    statement = """
    insert into chat_rooms (
        chat_room_id,
        channel_id,
        chat_room_status
    )
    select
        '{0}' as chat_room_id,
        channel_id,
        'non_accepted' as chat_room_status
    from 
        channels
    where
        channel_technical_id = '{1}'
    limit 1
    returning
        chat_room_id,
        channel_id,
        chat_room_status;
    """.format(
        chat_room_id,
        TELEGRAM_BOT_TOKEN
    )

    # Execute a previously prepared SQL query.
    try:
        cursor.execute(statement)
    except Exception as error:
        logger.error(error)
        sys.exit(1)

    # After the successful execution of the query commit your changes to the database.
    postgresql_db_connection.commit()

    # Determine information about the chat room from the database.
    chat_room_entry = cursor.fetchone()

    # Link a previously created chat room with a technical ID from telegram.
    statement = """
    insert into telegram_chat_rooms (
        chat_room_id,
        telegram_chat_id
    ) values (
        '{0}',
        '{1}'
    );
    """.format(
        chat_room_entry["chat_room_id"],
        telegram_chat_id
    )

    # Execute a previously prepared SQL query.
    try:
        cursor.execute(statement)
    except Exception as error:
        logger.error(error)
        sys.exit(1)

    # After the successful execution of the query commit your changes to the database.
    postgresql_db_connection.commit()

    # Prepare the SQL request that allows to get the list of departments that serve the specific this channel.
    statement = """
    select
        array_agg(distinct organization_id)::varchar[] as organizations_ids
    from
        channels
    left join channel_types on
        channels.channel_type_id = channel_types.channel_type_id
    left join channels_organizations_relationship on
        channels.channel_id = channels_organizations_relationship.channel_id
    where
        channels.channel_technical_id = '{0}'
    and
        lower(channel_types.channel_type_name) = lower('telegram')
    group by
        channels.channel_id,
        channel_types.channel_type_id
    limit 1;
    """.format(TELEGRAM_BOT_TOKEN.replace("'", "''"))

    # Execute a previously prepared SQL query.
    try:
        cursor.execute(statement)
    except Exception as error:
        logger.error(error)
        sys.exit(1)

    # After the successful execution of the query commit your changes to the database.
    postgresql_db_connection.commit()

    # Define the list of departments that can serve the specific channel.
    organizations_ids = cursor.fetchone()["organizations_ids"]

    # The cursor will be unusable from this point forward.
    cursor.close()

    # Prepare the CQL query statement that creates a new non accepted chat room in the Cassandra database.
    if len(organizations_ids) != 0:
        for organization_id in organizations_ids:
            cassandra_query = """
            insert into non_accepted_chat_rooms (
                organization_id,
                channel_id,
                chat_room_id,
                client_id
            ) values (
                {0},
                {1},
                {2},
                {3}
            );
            """.format(
                organization_id,
                chat_room_entry["channel_id"],
                chat_room_entry["chat_room_id"],
                client_id
            )
            statement = SimpleStatement(
                cassandra_query,
                consistency_level=ConsistencyLevel.LOCAL_QUORUM
            )

            # Execute a previously prepared CQL query.
            try:
                cassandra_db_connection.execute(statement)
            except Exception as error:
                logger.error(error)
                sys.exit(1)

    # Return the information about the specific chat room.
    return chat_room_entry


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


def create_chat_room_message(cassandra_db_connection, chat_room_id, client_id, channel_id, message_text):
    """
    Function name:
    create_chat_room_message

    Function description:
    The main task of this function is to add a new message from the client to the Cassandra database.
    """
    # The data type of the 'message_id' column is 'timeuuid'.
    message_id = uuid.uuid1()

    # Prepare the CQL query statement that creates a new message in the specific chat room.
    cassandra_query = """
    insert into chat_rooms_messages (
        chat_room_id,
        message_created_date_time,
        message_updated_date_time,
        message_deleted_date_time,
        message_is_sent,
        message_is_delivered,
        message_is_read,
        message_id,
        message_author_id,
        message_channel_id,
        message_type,
        message_text,
        message_content_url,
        quoted_message_id,
        quoted_message_author_id,
        quoted_message_channel_id,
        quoted_message_type,
        quoted_message_text,
        quoted_message_content_url
    ) values (
        {0},
        toTimestamp(now()),
        toTimestamp(now()),
        null,
        true,
        false,
        false,
        {1},
        {2},
        {3},
        'text',
        {4},
        null,
        null,
        null,
        null,
        null,
        null,
        null
    );
    """.format(
        chat_room_id,
        message_id,
        client_id,
        channel_id,
        'null' if message_text is None or len(message_text) == 0
        else "'{0}'".format(message_text.replace("'", "''"))
    )
    statement = SimpleStatement(
        cassandra_query,
        consistency_level=ConsistencyLevel.LOCAL_QUORUM
    )

    # Execute a previously prepared CQL query.
    try:
        cassandra_db_connection.execute(statement)
    except Exception as error:
        logger.error(error)
        sys.exit(1)

    # Return nothing.
    return None


def add_chat_room_member(postgresql_db_connection, chat_room_id, client_id):
    """
    Function name:
    add_chat_room_member

    Function description:
    The main task of this function is to add a client who writes to the telegram bot as a member of the chat room.
    """
    # With a dictionary cursor, the data is sent in a form of Python dictionaries.
    cursor = postgresql_db_connection.cursor(cursor_factory=RealDictCursor)

    # Prepare the SQL query statement that add client as a member of the specific chat room.
    statement = """
    insert into chat_rooms_users_relationship (
        chat_room_id,
        user_id
    ) values (
        '{0}',
        '{1}'
    );
    """.format(
        chat_room_id,
        client_id
    )

    # Execute a previously prepared SQL query.
    try:
        cursor.execute(statement)
    except Exception as error:
        logger.error(error)
        sys.exit(1)

    # After the successful execution of the query commit your changes to the database.
    postgresql_db_connection.commit()

    # The cursor will be unusable from this point forward.
    cursor.close()

    # Return nothing.
    return None


def activate_closed_chat_room(postgresql_db_connection, cassandra_db_connection, chat_room_id, channel_id, client_id):
    """
    Function name:
    activate_closed_chat_room

    Function description:
    The main task of this function is to add a client who writes to the telegram bot as a member of the chat room.
    """
    # With a dictionary cursor, the data is sent in a form of Python dictionaries.
    cursor = postgresql_db_connection.cursor(cursor_factory=RealDictCursor)

    # Prepare the SQL request that gives the id of the last operator in the chat room and id of the channel.
    statement = """
    select
        users.user_id as operator_id
    from
        chat_rooms_users_relationship
    left join users on
        chat_rooms_users_relationship.user_id = users.user_id
    left join chat_rooms on
        chat_rooms_users_relationship.chat_room_id = chat_rooms.chat_room_id
    where
        chat_rooms_users_relationship.chat_room_id = '{0}'
    and
        users.internal_user_id is not null
    and
        users.identified_user_id is null
    and
        users.unidentified_user_id is null
    order by
        chat_rooms_users_relationship.entry_created_date_time desc
    limit 1;
    """.format(chat_room_id)

    # Execute a previously prepared SQL query.
    try:
        cursor.execute(statement)
    except Exception as error:
        logger.error(error)
        sys.exit(1)

    # After the successful execution of the query commit your changes to the database.
    postgresql_db_connection.commit()

    # Define the variable with the ID of the operator.
    operator_id = cursor.fetchone()["operator_id"]

    # Prepare the SQL request that allows to get the list of departments that serve the specific this channel.
    statement = """
    select
        array_agg(distinct organization_id)::varchar[] as organizations_ids
    from
        channels
    left join channel_types on
        channels.channel_type_id = channel_types.channel_type_id
    left join channels_organizations_relationship on
        channels.channel_id = channels_organizations_relationship.channel_id
    where
        channels.channel_id = '{0}'
    group by
        channels.channel_id,
        channel_types.channel_type_id
    limit 1;
    """.format(channel_id)

    # Execute a previously prepared SQL query.
    try:
        cursor.execute(statement)
    except Exception as error:
        logger.error(error)
        sys.exit(1)

    # After the successful execution of the query commit your changes to the database.
    postgresql_db_connection.commit()

    # Define the variable with the IDs of the organizations.
    organizations_ids = cursor.fetchone()["organizations_ids"]

    # Prepare the CQL query statement that transfers chat room information from one table to another.
    for organization_id in organizations_ids:
        cassandra_query = """
        insert into non_accepted_chat_rooms (
            organization_id,
            channel_id,
            chat_room_id,
            client_id
        ) values (
            {0},
            {1},
            {2},
            {3}
        );
        """.format(
            organization_id,
            channel_id,
            chat_room_id,
            client_id
        )
        statement = SimpleStatement(
            cassandra_query,
            consistency_level=ConsistencyLevel.LOCAL_QUORUM
        )

        # Execute a previously prepared CQL query.
        try:
            cassandra_db_connection.execute(statement)
        except Exception as error:
            logger.error(error)
            sys.exit(1)

    # Prepare the CQL query statement that deletes chat room information from 'completed_chat_rooms' table.
    cassandra_query = """
    delete from
        completed_chat_rooms
    where
        operator_id = {0}
    and
        channel_id = {1}
    and
        chat_room_id = {2};
    """.format(
        operator_id,
        channel_id,
        chat_room_id
    )
    statement = SimpleStatement(
        cassandra_query,
        consistency_level=ConsistencyLevel.LOCAL_QUORUM
    )

    # Execute a previously prepared CQL query.
    try:
        cassandra_db_connection.execute(statement)
    except Exception as error:
        logger.error(error)
        sys.exit(1)

    # Prepare the SQL query statement that update the status of the specific chat room.
    statement = """
    update
        chat_rooms
    set
        chat_room_status = 'non_accepted'
    where
        chat_room_id = '{0}';
    """.format(chat_room_id)

    # Execute a previously prepared SQL query.
    try:
        cursor.execute(statement)
    except Exception as error:
        logger.error(error)
        sys.exit(1)

    # After the successful execution of the query commit your changes to the database.
    postgresql_db_connection.commit()

    # The cursor will be unusable from this point forward.
    cursor.close()

    # Return nothing.
    return None


def get_client(postgresql_db_connection, telegram_username):
    """
    Function name:
    get_client

    Function description:
    The main purpose of this function is to get information about the client.
    """
    # With a dictionary cursor, the data is sent in a form of Python dictionaries.
    cursor = postgresql_db_connection.cursor(cursor_factory=RealDictCursor)

    # Check if the database has information for the specific telegram username.
    statement = """
    select
        users.user_id
    from
        users
    left join identified_users on
        users.identified_user_id = identified_users.identified_user_id
    where
        users.identified_user_id is not null
    and
        users.internal_user_id is null
    and
        users.unidentified_user_id is null
    and
        identified_users.telegram_username = '{0}'
    limit 1;
    """.format(telegram_username)

    # Execute a previously prepared SQL query.
    try:
        cursor.execute(statement)
    except Exception as error:
        logger.error(error)
        sys.exit(1)

    # After the successful execution of the query commit your changes to the database.
    postgresql_db_connection.commit()

    # Determine information about the chat room from the database.
    user_entry = cursor.fetchone()
    user_id = None
    if user_entry is not None:
        user_id = user_entry["user_id"]

    # The cursor will be unusable from this point forward.
    cursor.close()

    # Return the id of the client.
    return user_id


def update_chat_room_last_message(postgresql_db_connection, cassandra_db_connection, message_text, channel_id,
                                  chat_room_id):
    """
    Function name:
    update_chat_room_last_message

    Function description:
    The main purpose of this function is to update the last chat room message.
    """
    # With a dictionary cursor, the data is sent in a form of Python dictionaries.
    cursor = postgresql_db_connection.cursor(cursor_factory=RealDictCursor)

    # Prepare the SQL request that gives the id of the last operator in the chat room and id of the channel.
    statement = """
    select
        users.user_id as operator_id
    from
        chat_rooms_users_relationship
    left join users on
        chat_rooms_users_relationship.user_id = users.user_id
    left join chat_rooms on
        chat_rooms_users_relationship.chat_room_id = chat_rooms.chat_room_id
    where
        chat_rooms_users_relationship.chat_room_id = '{0}'
    and
        users.internal_user_id is not null
    and
        users.identified_user_id is null
    and
        users.unidentified_user_id is null
    order by
        chat_rooms_users_relationship.entry_created_date_time desc
    limit 1;
    """.format(chat_room_id)

    # Execute a previously prepared SQL query.
    try:
        cursor.execute(statement)
    except Exception as error:
        logger.error(error)
        sys.exit(1)

    # After the successful execution of the query commit your changes to the database.
    postgresql_db_connection.commit()

    # Define the variable with the ID of the operator.
    operator_id = cursor.fetchone()["operator_id"]

    cassandra_query = """
    update
        accepted_chat_rooms
    set
        last_message_content = {1},
        last_message_date_time = toTimestamp(now())
    where
        operator_id = {2}
    and
        channel_id = {3}
    and
        chat_room_id = {4}
    if exists;
    """.format(
        'null'
        if message_text is None or len(message_text) == 0
        else "'{0}'".format(message_text.replace("'", "''")),
        operator_id,
        channel_id,
        chat_room_id
    )
    statement = SimpleStatement(
        cassandra_query,
        consistency_level=ConsistencyLevel.LOCAL_QUORUM
    )

    # Execute a previously prepared CQL query.
    try:
        cassandra_db_connection.execute(statement)
    except Exception as error:
        logger.error(error)
        sys.exit(1)

    # Return nothing.
    return {
        "statusCode": 200
    }
