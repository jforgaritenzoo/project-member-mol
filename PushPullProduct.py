from timeit import default_timer as timer
from datetime import datetime
from time import sleep
from app.methods import monitor, total_rows, load, extract, new_trunc_table, new_extract
from app.connection import (
    get_klik_conn,
    get_local_conn,
    get_crm_conn,
    get_app_conn,
    get_source_conn,
)

now = datetime.now()
day_month_year = now.strftime("%d-%m-%Y")
fulldate = now.strftime("%c")

# TIMER
start = timer()

# CHANGE THIS VARIABLE FOR DIFF TABLE
query_table_name = "products"

# OPEN QUERIES FILE
with open(f"queries/{query_table_name}.sql", "r") as query_file:
    query = query_file.read()

# *SCRIPT EXECUTION

table_name = query_table_name
print(f"Script for PushPull{table_name} is running at : {fulldate} !")

sleep_time = 2
num_retries = 1
for x in range(0, num_retries):
    try:
        # klik_conn = get_klik_conn()
        app_conn = get_app_conn()
        crm_conn = get_crm_conn()
        local_conn = get_local_conn()

        print(total_rows(local_conn, table_name))  # Printing before total data
        data = new_extract(crm_conn, query, chunksize=32500)
        # data = extract(crm_conn, query, table_name, chunksize=32500)  # Extracting data
        if data.empty:
            monitor(app_conn, 0, table_name)
            break
        new_trunc_table(local_conn, table_name)
        load(local_conn, data, table_name, 1500)

        stop = timer()
        total_time = round(stop - start, 2)
        print(f"Total time for code run: {total_time} s")

        monitor(app_conn, total_time, table_name)

        str_error = None

    except Exception as e:
        monitor(app_conn, 0, table_name)
        str_error = str(e)
        print(str_error)

    except KeyboardInterrupt:
        monitor(app_conn, 0, table_name, intr=True)
        str_error = None
        print("Process Stopped!")

    if str_error:
        print("\nError : Retrying....")
        sleep(sleep_time)
        sleep_time *= 2
    else:
        print("\nProccess Finished")
        # klik_conn.close()
        break
