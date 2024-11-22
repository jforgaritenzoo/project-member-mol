import json
import subprocess
from .methods import consume_api
from datetime import datetime
from typing import Optional
from fastapi import FastAPI, Depends, HTTPException, WebSocket
from fastapi.encoders import jsonable_encoder
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from . import crud, models, schemas
from .schemas import ApiResponse, Meta
from .databaseapi import SessionLocal, app_conn
from sqlalchemy.orm import Session
from fastapi.middleware.cors import CORSMiddleware
from subprocess import run, PIPE

models.Base.metadata.create_all(bind=app_conn)

apilog = FastAPI()

apilog.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Change this in production to restrict allowed origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Dependency
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@apilog.get("/")
async def root():
    message = {"message": "This is the main page for the API"}
    return JSONResponse(content=message)

    # for testing webpage use below
    # return HTMLResponse(html)


@apilog.get("/runscript")
async def runscript(job_name: str = ""):
    python_executable = "D:\\Code\\Test1\\pull-memberonline\\venv\\Scripts\\python.exe"
    script_path = f"D:\\Code\\Test1\\pull-memberonline\\PushPull{job_name}.py"
    try:
        result = run(
            [python_executable, script_path],
            stdout=PIPE,
            stderr=PIPE,
            text=True,
            cwd="D:\\Code\\Test1\\pull-memberonline",
        )
        if result.stdout and not result.stderr:
            return {"msg": f"Pushpull for {job_name} job executed successfully"}
        else:
            raise HTTPException(
                status_code=500,
                detail=f"Pushpull for {job_name} job failed to execute: {e}",
            )
        # return {"stdout": result.stdout, "stderr": result.stderr}
    except subprocess.CalledProcessError as e:
        raise HTTPException(status_code=500, detail=f"Error executing script: {e}")


@apilog.get("/log/", response_model=ApiResponse)
def read_logs(
    skip: int = 0,
    limit: int = 100,
    keyword: Optional[str] = None,
    db: Session = Depends(get_db),
):
    if keyword:
        # Fetch logs based on the keyword filter
        logs = crud.get_logs_filter(db, keyword, skip=skip, limit=limit)
    else:
        # Fetch all logs
        logs = crud.get_logs(db, skip=skip, limit=limit)

    log_dicts = [log.__dict__ for log in logs]

    response = ApiResponse(
        status="success",
        data=log_dicts,
        meta=Meta(timestamp=datetime.utcnow(), version="1.0"),  # Use ISO 8601 format
    )

    return response


@apilog.get("/consume")
async def consume_data():
    messages = consume_api()
    if messages:
        return {"status": "success", "messages": messages}
    else:
        return {"status": "empty", "messages": "No messages available in the queue."}
