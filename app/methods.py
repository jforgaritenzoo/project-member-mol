import json, logging, time, pika, os, pymysql, pandas as pd
from sqlalchemy import Float, text, Connection
from sqlalchemy.orm import Session, sessionmaker
from timeit import default_timer as timer
from datetime import datetime

# Uncomment for printing the log for rabbimq
logging.basicConfig(level=logging.INFO)


class Log:
    def __init__(self) -> None:
        self._extracted = 0
        self._inserted = 0
        self._flag = 0

    @property
    def extracted(self):
        # print("extract get called")
        return self._extracted

    @property
    def inserted(self):
        # print("inserted get called")
        return self._inserted

    @property
    def flag(self):
        # print("flag get called")
        return self._flag

    @extracted.setter
    def extracted(self, a):
        # print('setter extract called')
        self._extracted = a

    @inserted.setter
    def inserted(self, a):
        # print('setter insert called')
        self._inserted = a

    @flag.setter
    def flag(self, a):
        # print('setter flag called')
        self._flag = a


log = Log()


# COUNT TOTAL DATA
def total_rows(engine: Connection, tablename: str, before=True) -> str:
    try:
        target = pd.read_sql(f"SELECT COUNT(*) as Total FROM {tablename}", engine)
        count_target = int(target["Total"].to_string(index=False))
        if before:
            message = f"Total Data before inserted : {count_target:,} rows\n"
        else:
            message = f"Total Data after inserted : {count_target:,} rows"
    except Exception as e:
        message = f"Error Message: {str(e)}"

    if "1146" in str(message).lower():
        return "ERROR 1146 " + message
    else:
        return message


def total_rownum(engine: Connection, tablename: str) -> int:
    try:
        target = pd.read_sql(f"SELECT COUNT(*) as Total FROM {tablename}", engine)
        count_target = int(target["Total"].to_string(index=False))
        log.flag = 1
    except Exception as e:
        log.flag = 2
        print(f"Error on total_rownum: {str(e)}")
    return count_target


# EXTRACTING
def extract(
    connection: Connection,
    query: str,
    tablename="default",
    chunksize=25000,
    local=False,
) -> pd.DataFrame:

    # Variable for the codes
    now = datetime.now()
    yearmmdd = now.strftime("%Y-%m-%d")
    start = timer()
    all_data = []
    check = True

    # print(connection)

    # Checking Query/Connection
    try:
        for chunked in pd.read_sql(query, connection, chunksize=chunksize):
            try:
                all_data.append(chunked)
                print(f"Dataframe with {len(chunked)} rows is extracted")
            except Exception as e:
                print(f"Error: {str(e)}")
                check = False
                break
    except Exception as e:
        print(f"Error: {str(e)}")
        log.flag = 3
        check = False

    # connection.close()

    stop = timer()
    print(f"Time elapsed for the extraction : {round(stop - start,2)} s")

    if all_data and check:
        log.flag = 1
        source = pd.concat(all_data, ignore_index=True)

        # Backup csv
        # if local:
        #     file_name = f"file/{yearmmdd}-{tablename}-extract-backup.csv"
        # else:
        #     file_name = f"file/{yearmmdd}-{tablename}-extract-klik-backup.csv"
        # source.to_csv(file_name, index=False)

        count_source = len(source)
        log.extracted = count_source
        print(f"Total Data extracted : {count_source:,} rows\n")
        print("Extraction Success, proceeding to truncating table")

        # resource usage
        # print("Final resource usage after extraction:")
        # resource_usage()

        return source
    else:
        log.flag = 3
        print("Error, no data extracted.\n")
        return pd.DataFrame()


# USING RAW CONNECTION
def new_extract(
    connection: Connection,
    query: str,
    tablename="default",
    chunksize=25000,
    local=False,
) -> pd.DataFrame:

    # Variable for the codes
    now = datetime.now()
    yearmmdd = now.strftime("%Y-%m-%d")
    start = timer()
    all_data = []
    check = True

    try:
        result = connection.execution_options(stream_results=True).execute(text(query))
        while True:
            batch = result.fetchmany(chunksize)
            if not batch:
                break
            print(f"Data with {len(batch)} rows is extracted")
            df = pd.DataFrame(batch, columns=result.keys())  # Convert to DataFrame
            all_data.append(df)

        big_df = pd.concat(all_data, ignore_index=True)
        # print(big_df)
    except Exception as e:
        print(f"Error: {str(e)}")
        log.flag = 3
        check = False

    stop = timer()
    print(f"Time elapsed for the extraction : {round(stop - start,2)} s")

    if all_data and check:
        log.flag = 1

        # Backup csv
        # if local:
        #     file_name = f"file/{yearmmdd}-{tablename}-extract-backup.csv"
        # else:
        #     file_name = f"file/{yearmmdd}-{tablename}-extract-klik-backup.csv"
        # big_df.to_csv(file_name, index=False)

        count_source = len(big_df)
        log.extracted = count_source
        print(f"Total Data extracted : {count_source:,} rows\n")
        print("Extraction Success, proceeding to truncating table")

        # resource usage
        # print("Final resource usage after extraction:")
        # resource_usage()

        return big_df
    else:
        log.flag = 3
        print("Error, no data extracted.\n")
        return pd.DataFrame()


# CLEANING/TRUNCATE DATA
def new_trunc_table(connection: Connection, table_name: str) -> None:
    try:
        # Use a context manager for executing raw SQL queries
        truncate_query = text(f"TRUNCATE TABLE {table_name}")
        connection.execute(truncate_query)
        print(f"{table_name} is now emptied")
    except Exception as e:
        if "1146" in str(e).lower():
            print(f"ERROR 1146, Creating the {table_name} table on load : {str(e)}")
        else:
            print(f"Other error occured : {str(e)}")
        log.flag = 5
    finally:
        connection.commit()


def trunc_table(connection: Connection, table_name: str) -> None:
    with Session(connection) as session:
        try:
            truncate_query = text(f"TRUNCATE TABLE {table_name}")
            connection.execute(truncate_query)
            print(f"{table_name} is now emptied")
            session.commit()
        except Exception as e:
            print(f"Error on Truncating table: {str(e)}")
            session.rollback()  # Roll back the transaction on error
        finally:
            session.close()
    connection.commit()


def transform(csv: str) -> pd.DataFrame:
    data = pd.read_csv(f"file/{csv}")
    return data


def load_klik(
    conn: Connection, df: pd.DataFrame, tablename: str, chunksize=500
) -> None:
    #  *Count TIME FOR ELAPSED CODE RUNNING
    start = timer()

    # Loading time!
    Session = sessionmaker(bind=conn)
    session = Session()
    try:
        with session.begin():
            df.to_sql(
                tablename,
                conn,
                if_exists="append",
                chunksize=chunksize,
                index=False,
            )
            session.commit()
    except Exception as e:
        print(f"Error: {str(e)}")
        session.rollback()

    #  *TIME FOR ELAPSED CODE RUNNING
    stop = timer()
    print(f"Time elapsed for the loading : {round(stop - start,2)} s")

    print(total_rows(conn, tablename, False))

    conn.commit()

# LOADING DATA for TESTING
def load(
    connection: Connection, df: pd.DataFrame, tablename: str, chunksize=500
) -> None:
    #  *Count TIME FOR ELAPSED CODE RUNNING
    start = timer()

    with Session(connection) as session:
        try:
            df.to_sql(
                tablename,
                connection,
                if_exists="append",
                chunksize=chunksize,
                index=False,
            )
            session.commit()
            log.flag = 1

            log.inserted = total_rownum(connection, tablename)
            print(f"Total data after inserted : {total_rownum(connection, tablename):,} rows")

        except Exception as e:

            if "1146" in str(e).lower():
                error = f"Error 1146: {str(e)}"
            else:
                error = f"Error on load: {str(e)}"

            log.flag = 4
            print(error)
            session.rollback()
            connection.close()

    #  *TIME FOR ELAPSED CODE RUNNING
    stop = timer()
    print(f"Time elapsed for the loading : {round(stop - start,2)} s")

    connection.commit()


# LOAD THE PROCESS TO THE MONITORING DB
def monitor(conn: Connection, time: Float, table_name: str, intr=False):

    # CONNECTING TO RABBITMQ SERVER
    # connection, channel = create_connection_with_url(os.getenv("CLOUDAMQP_URL"))
    try:
        date = datetime.now().strftime("%Y-%m-%d")
        timestamp = datetime.now().strftime("%H:%M:%S")
        fetched = log.extracted
        loaded = log.inserted
        runtime = time
        job_name = table_name + "_job"
        if intr:
            flag = 0
        else:
            flag = log.flag

        if flag == 1:
            msg = "Success"
        elif flag == 2:
            msg = "Table doesn't exist"
        elif flag == 3:
            msg = "Extraction failed"
        elif flag == 4:
            msg = "Loading failed"
        elif flag == 5:
            msg = "Truncating failed"
        else:
            msg = "Error"

        data = [date, timestamp, msg, fetched, loaded, runtime, job_name, flag]

        df = pd.DataFrame(
            [data],
            columns=[
                "date_running",
                "time_running",
                "message",
                "rows_fetched",
                "rows_inserted",
                "runtime",
                "job_name",
                "flag",
            ],
        )

        # BUILD THE MESSAGE TO SEND TO RABBITMQ PUBLISHER
        # df_json = df.to_json(orient='records', index=False)

        # message = {
        #     "event_type": "job_running_status",  # Event type indicating the nature of the message
        #     "data": df_json
        # }

        with Session(conn) as session:
            try:
                df.to_sql(
                    "konsolidasi_log",
                    conn,
                    if_exists="append",
                    index=False,
                )
                # send_message(channel, json.dumps(message), "fanout", "amq.fanout", "")
                session.commit()
            except Exception as e:
                print(f"Error on monitoring:{e}")
                session.rollback()
    except Exception as e:
        print(f"Error during monitoring: {e}")
        raise

# RABBIT MQ PUBLISH & CONSUME
def create_connection_with_url(url):
    """
    Establish connection to RabbitMQ and return the connection and channel.
    """
    params = pika.URLParameters(url)
    connection = pika.BlockingConnection(params)  # Connect to RabbitMQ
    channel = connection.channel()  # Start a channel

    for i in range(3):
        try:
            return connection, channel
        except pika.exceptions.AMQPConnectionError as e:
            logging.error(f"Connection failed: {e}. Retrying {i+1}/{3}...")
            time.sleep(5)
    logging.error("Unable to connect after retries. Exiting.")
    raise SystemExit(1)

def send_message(
    channel, message, exchange_type, exchange_name="default", routing_key="sim_test"
):
    """
    Send a message to RabbitMQ queue.
    """
    channel.exchange_declare(
        exchange=exchange_name, exchange_type=exchange_type, durable=True
    )

    channel.queue_bind(exchange=exchange_name, queue="sim_test")

    try:
        channel.basic_publish(
            exchange=exchange_name,  # Default exchange
            routing_key=routing_key,  # Queue name
            body=json.dumps(message),
            properties=pika.BasicProperties(
                delivery_mode=2,  # Make the message persistent
            ),
        )
        logging.info(" [x] Message sent: %s", message)
    except Exception as e:
        logging.error(f"Error sending message: {e}")

def callback(ch, method, properties, body):
    """
    Callback function for processing messages.
    """
    try:
        process_message(body.decode())  # Decode message before processing
        ch.basic_ack(
            delivery_tag=method.delivery_tag
        )  # Manually acknowledge the message
    except Exception as e:
        logging.error(f"Error processing message: {e}")
        ch.basic_nack(
            delivery_tag=method.delivery_tag
        )  # Negative acknowledgment if failed

def process_message(msg):
    """
    Simulate processing of a message.
    """
    logging.info("User processing started.")
    logging.info(" [x] Received: %s", msg)

    # Simulate time-consuming work (e.g., processing a PDF)
    time.sleep(2)  # Simulate processing delay (e.g., 5 seconds)

    # Simulate a processing error
    # raise Exception("Error processing the PDF")

    logging.info("User processing finished.")
    return

def consume_api():
    host = os.getenv("RMQ_KLIK_HOST")
    user = os.getenv("RMQ_KLIK_USER")
    password = os.getenv("RMQ_KLIK_PASS")
    vhost = os.getenv("RMQ_KLIK_VHOST")
    port = 5672

    # Create credentials and connection parameters
    creds = pika.PlainCredentials(user, password)
    params = pika.ConnectionParameters(
        host=host,
        port=port,
        virtual_host=vhost,
        credentials=creds,
        socket_timeout=60,
    )
        
    connection = pika.BlockingConnection(params)  # Connect to RabbitMQ
    channel = connection.channel()  # Start a channel

    # Declare the queue (if it doesn't exist already)
    channel.queue_declare(queue="sim_mol", durable=True)

    messages = []
    while True:
        method_frame, properties, body = channel.basic_get(
            queue="sim_mol", auto_ack=True
        )

        if method_frame:  # A message was retrieved
            logging.info(f"Message received: {body}")
            messages.append(body.decode("utf-8"))
        else:  # No more messages in the queue
            break

    connection.close()
    return messages

def consume_api_local():
    host = os.getenv("RMQ_HOST")
    user = os.getenv("RMQ_USER")
    password = os.getenv("RMQ_PASS")
    vhost = os.getenv("RMQ_VHOST")
    port = 5672

    # Create credentials and connection parameters
    creds = pika.PlainCredentials(user, password)
    params = pika.ConnectionParameters(
        host=host,
        port=port,
        virtual_host=vhost,
        credentials=creds,
        socket_timeout=60,
    )
        
    connection = pika.BlockingConnection(params)  # Connect to RabbitMQ
    channel = connection.channel()  # Start a channel

    # Declare the queue (if it doesn't exist already)
    channel.queue_declare(queue="sim_mol", durable=True)

    messages = []
    while True:
        method_frame, properties, body = channel.basic_get(
            queue="sim_mol", auto_ack=True
        )

        if method_frame:  # A message was retrieved
            logging.info(f"Message received: {body}")
            messages.append(body.decode("utf-8"))
        else:  # No more messages in the queue
            break

    connection.close()
    return messages
