from app.connection import get_app_conn
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

app_conn = get_app_conn()

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=app_conn)

Base = declarative_base()