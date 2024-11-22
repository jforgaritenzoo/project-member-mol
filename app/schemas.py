from pydantic import BaseModel
from datetime import date, time, datetime
from typing import List, Optional


class LogBase(BaseModel):
    job_name: Optional[str]
    date_running: date
    time_running: time
    message: str
    rows_fetched: int
    rows_inserted: int
    runtime: float
    job_name: str
    flag: str


class Log(LogBase):
    id: int

    class Config:
        orm_mode = True


class Meta(BaseModel):
    timestamp: datetime
    version: str


class ApiResponse(BaseModel):
    status: str
    data: List[Log]
    meta: Meta


class LogCreate(LogBase):
    pass
