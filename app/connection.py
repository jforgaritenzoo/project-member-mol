import os
from sqlalchemy import create_engine
import oracledb
from dotenv import load_dotenv

load_dotenv()
# Oracle basiclite for thick connection
oracledb.init_oracle_client(
    lib_dir="D:\Code\instantclient-basiclite-windows.x64-23.6.0.24.10\instantclient_23_6"
)

# *DATABASE CREDENTIAL MYSQL LOCAL, IGRCRM
host = os.getenv("HOST_KLIKS")
user = os.getenv("USER_KLIKS")
password = os.getenv("PASSWORD_KLIKS")
database = os.getenv("DATABASE_KLIKS")

user2 = os.getenv("USER_LOCAL")
password2 = os.getenv("PASSWORD_LOCAL")
host2 = os.getenv("HOST_LOCAL")
database2 = os.getenv("DATABASE_LOCAL")
databasetest = os.getenv("HOST_LOCAL_TEST")

user3 = os.getenv("USER_SOURCE_DEV")
password3 = os.getenv("PASSWORD_SOURCE_DEV")
host3 = os.getenv("HOST_SOURCE_DEV")

usercrm = os.getenv("USER_SOURCE_PROD")
passwordcrm = os.getenv("PASSWORD_SOURCE_PROD")
hostcrm = os.getenv("HOST_SOURCE_PROD")

# CREDS FOR LOGGING APP
database4 = os.getenv("DATABASE_LOCAL_LOGGING")


# # *ENGINE CONNECTION INORDER MYSQL, LOCAL, IGRCRM
def get_klik_conn():
    try:
        klik_conn = create_engine(
            "mysql+pymysql://{0}:{1}@{2}/{3}".format(user, password, host, database)
        ).connect()

        return klik_conn
    except Exception as e:
        print(f"Error connecting to Databases: {e}")


def get_source_conn():
    try:
        source_conn = create_engine(
            url="oracle+oracledb://{0}:{1}@{2}:1521/orcl".format(
                user3, password3, host3
            )
        ).connect()

        return source_conn
    except Exception as e:
        print(f"Error connecting to Databases: {e}")


def get_crm_conn():
    try:
        crm_conn = create_engine(
            url="oracle+oracledb://{0}:{1}@{2}:1521/igrcrm".format(
                usercrm, passwordcrm, hostcrm
            )
        ).connect()

        return crm_conn
    except Exception as e:
        print(f"Error connecting to Databases: {e}")
        raise


def get_local_conn():
    try:
        localdb_conn = create_engine(
            url="mysql+pymysql://{0}:{1}@{2}/{3}".format(
                user2, password2, host2, database2
            )
        ).connect()

        return localdb_conn
    except Exception as e:
        print(f"Error connecting to Databases: {e}")


def get_app_conn():
    try:
        app_conn = create_engine(
            url="mysql+pymysql://{0}:{1}@{2}/{3}".format(
                user2, password2, host2, database4
            )
        ).connect()

        return app_conn
    except Exception as e:
        print(f"Error connecting to Databases: {e}")
