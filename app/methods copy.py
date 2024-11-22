import os
import psutil
import pandas as pd
import pymongo as pm
from pymongo import errors
from sqlalchemy import Float, text, Connection
from sqlalchemy.orm import Session
from timeit import default_timer as timer
from datetime import datetime

# COUNT TOTAL DATA
def total_rows(engine: Connection, tablename: str, before=True) -> str:
    try:
        target = pd.read_sql(f"SELECT COUNT('id') Total FROM {tablename}", engine)
        count_target = int(target["Total"].to_string(index=False))
        if before == 1:
            message = f"Total Data before inserted : {count_target:,} rows\n"
        else:
            message = f"Total Data after inserted : {count_target:,} rows"
    except Exception as e:
        print(f"Error: {str(e)}")
        message = f"Table {tablename} is Created"

    return message


def total_rownum(engine: Connection, tablename: str) -> int:
    try:
        target = pd.read_sql(f"SELECT COUNT('id') Total FROM {tablename}", engine)
        count_target = int(target["Total"].to_string(index=False))
    except Exception as e:
        print(f"Error: {str(e)}")

    return count_target


# CLEANING/TRUNCATE DATA
def trunc_table(connection: Connection, table_name: str) -> None:
    with Session(connection) as session:
        try:
            delete_query = text(f"TRUNCATE TABLE {table_name}")
            connection.execute(delete_query)
            print(f"{table_name} is now emptied")
            session.commit()
        except Exception as e:
            print(f"Error: {str(e)}")
            session.rollback()
        finally:
            session.close()
    connection.commit()


def resource_usage():
    process = psutil.Process(os.getpid())
    cpu_usage = psutil.cpu_percent(interval=1)
    memory_info = process.memory_info()
    memory_usage = memory_info.rss / (1024 * 1024)
    net_io = psutil.net_io_counters()
    bytes_sent = net_io.bytes_sent / (1024 * 1024) 
    bytes_recv = net_io.bytes_recv / (1024 * 1024) 
    
    print(f"CPU Usage: {cpu_usage}%")
    print(f"Memory Usage: {memory_usage:.2f} MB")
    print(f"Bytes Sent: {bytes_sent:.2f} MB")
    print(f"Bytes Received: {bytes_recv:.2f} MB")

# EXTRACTING
def extract(
    connection: Connection,
    query: str,
    chunksize=25000,
    tablename="default",
    local=False,
) -> pd.DataFrame:
    now = datetime.now()
    yearmmdd = now.strftime("%Y-%m-%d")

    start = timer()

    all_data = []
    chunk_count = 0
    
    # resource usage
    # print("Initial resource usage:")
    # resource_usage()

    # EXTRACT and TRANSFORM
    for chunked in pd.read_sql(query, connection, chunksize=chunksize):
        try:
            all_data.append(chunked)
            chunk_count += 1
            # print(f"Dataframe with {len(chunked)} rows is extracted")
            # resource_usage()
        except Exception as e:
            print(f"Error: {str(e)}")
            log.flag = 0

    stop = timer()
    print(f"Time elapsed for the extraction : {round(stop - start,2)} s")

    connection.close()

    if all_data:
        source = pd.concat(all_data, ignore_index=True)

        # Backup csv
        if local:
            file_name = f"file/{yearmmdd}-{tablename}-extract-backup.csv"
        else:
            file_name = f"file/{yearmmdd}-{tablename}-extract-klik-backup.csv"
        source.to_csv(file_name, index=False)

        count_source = len(source)
        log.extracted = count_source
        print(f"Total Data extracted : {count_source:,} rows\n")
        log.flag = 1
        
        # resource usage
        # print("Final resource usage after extraction:")
        # resource_usage()
        
        return source
    else:
        print("No data extracted.\n")
        log.flag = 0
        return pd.DataFrame()


# LOADING DATA
def load_local(
    connection: Connection, df: pd.DataFrame, tablename: str, chunksize=500
) -> None:
    #  *Count TIME FOR ELAPSED CODE RUNNING
    start = timer()

    # resource monitoring
    # print("\nResource usage before loading:")
    # resource_usage()
    
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
        except Exception as e:
            print(f"Error: {str(e)}")
            log.flag = 0
            session.rollback()
            connection.close()

    #  *Stop TIME FOR ELAPSED CODE RUNNING
    stop = timer()
    print(f"Time elapsed for the loading : {round(stop - start,2)} s")

    log.inserted = total_rownum(connection, tablename)
    print(f"Total data after inserted : {total_rownum(connection, tablename):,} rows")
    
    # resource monitoring
    print("Resource usage after loading:")
    resource_usage()

    connection.commit()


# Log for monitoring
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

def monitor(conn: Connection, time: Float, tablename: str):
    try:
        date = datetime.now().strftime("%Y-%m-%d")
        fetched = log.extracted
        loaded = log.inserted
        runtime = time
        job_name = tablename + "_job"
        flag = log.flag
        if log.flag == 1:
            msg = "Success"
        else:
            msg = "Failed"
        data = [date, msg, fetched, loaded, runtime, job_name, flag]

        df = pd.DataFrame(
            [data],
            columns=[
                "date_running",
                "message",
                "rows_fetched",
                "rows_inserted",
                "runtime",
                "job_name",
                "flag",
            ],
        )

        with Session(conn) as session:
            try:
                df.to_sql(
                    "konsolidasi_log",
                    conn,
                    if_exists="append",
                    index=False,
                )
                session.commit()
            except Exception as e:
                print(f"Error: {str(e)}")
                session.rollback()
    except Exception as e:
        print(e)


# MONGO METHOD
def count_doc(client: pm.MongoClient, collection_name: str) -> int:
    try:
        collection = client[collection_name]
        count_documents = collection.count_documents({})
        # print(count_documents)
        return count_documents
    except Exception as e:
        print(f"Error: {str(e)}")
        raise


def load_mongo(client: pm.MongoClient, df: pd.DataFrame, collection_name: str) -> None:
    start = timer()

    # resource monitoring
    print("\nResource usage before loading:")
    # resource_usage()

    try:
        collection = client[collection_name]

        data = df.to_dict(orient="records")
        collection.insert_many(data)

        print("Data inserted successfully")
        # resource_usage()
    except Exception as e:
        print(f"Error: {str(e)}")

    stop = timer()
    print(f"Time elapsed for loading: {round(stop - start, 2)} seconds")


def trunc_collection(client: pm.MongoClient, collection_name: str) -> None:
    try:
        client.drop_collection(collection_name)
        print(f"{collection_name} is now emptied")
    except errors.PyMongoError as e:
        print(f"Error: {str(e)}")
