from timeit import default_timer as timer
from datetime import datetime
from time import sleep
from app.methods import monitor, trunc_table, total_rows, load_local,extract
from app.connection import localdb_conn, source_conn, app_conn

now = datetime.now()
day_month_year = now.strftime("%d-%m-%Y")
fulldate = now.strftime("%c")

# TIMER
start = timer()

# OPEN QUERIES FILE
with open('queries/stocks.sql', 'r') as query_file:
    query_stocks = query_file.read()


# *SCRIPT EXECUTION

table_name = str("stock")
print(f"Script for PushPull{table_name} is running at : {fulldate} !")

sleep_time = 2
num_retries = 2
for x in range(0, num_retries):
    try:
        print(total_rows(localdb_conn, table_name))  # Printing before total data
        data = extract(source_conn, query_stocks, 32500)  # Extracting data
        if data.empty:
            monitor(app_conn,0,table_name)
            break
        trunc_table(localdb_conn, table_name)
        load_local(localdb_conn, data, table_name, 1500)  # Load to mysql local

        stop = timer()
        total_time = round(stop - start, 2)
        print(f"Total time for code run: {total_time} s")

        monitor(app_conn,total_time,table_name)
        
        str_error = None
    
    except KeyboardInterrupt :
        monitor(app_conn,0, table_name,intr=True)
        print("Process stopped!")
    
    except Exception as e:
        monitor(app_conn,0,table_name)
        str_error = str(e)
        print(str_error)

    if str_error:
        print("\nRetry....")
        sleep(sleep_time)
        sleep_time *= (
            2
        )
    else:
        break
