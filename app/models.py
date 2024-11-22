from sqlalchemy import CHAR, Column, Date, Double, Integer, String, Time
from .databaseapi import Base


class Log(Base):
    __tablename__ = "konsolidasi_log"
    
    id = Column(Integer, primary_key=True)
    date_running = Column(Date)
    time_running = Column(Time)
    message = Column(String(255))
    rows_fetched = Column(Integer)
    rows_inserted = Column(Integer)
    runtime = Column(Double)
    job_name = Column(String(255))
    flag = Column(CHAR(1))