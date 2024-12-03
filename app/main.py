import json
import subprocess
import os
from pathlib import Path
from .methods import consume_api, consume_api_local
from datetime import datetime
from typing import Optional
from fastapi import FastAPI, Depends, Form, HTTPException, WebSocket
from fastapi.encoders import jsonable_encoder
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from . import crud, models, schemas
from .schemas import ApiResponse, Meta, ScriptRequest
from .databaseapi import SessionLocal, app_conn
from sqlalchemy.orm import Session
from fastapi.middleware.cors import CORSMiddleware
from subprocess import run, PIPE
from pydantic import BaseModel

models.Base.metadata.create_all(bind=app_conn)

apilog = FastAPI()

apilog.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
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


@apilog.get("/runscript")
async def runscript(job_name: str = ""):
    root_dir = Path(__file__).resolve().parent.parent
    venv_dir = root_dir / "venv" / "Scripts" / "python.exe"
    script_path = root_dir / f"PushPull{job_name}.py"

    try:
        result = run(
            [venv_dir, script_path],
            stdout=PIPE,
            stderr=PIPE,
            text=True,
            cwd=str(root_dir),
        )

        if result.returncode != 0:
            raise HTTPException(
                status_code=500,
                detail=f"Error running script {job_name}: {result.stderr}",
            )

        # VERSION TWO
        # # Parse script's stdout
        logs = result.stdout.strip().splitlines()
        errors = []

        for line in logs:
            try:
                log_entry = json.loads(line)
                if log_entry.get("status") == "error":
                    errors.append(log_entry)
            except json.JSONDecodeError:
                continue

        if errors:
            raise HTTPException(
                status_code=400,
                detail=f"Script executed with errors: {errors}",
            )

        return {
            "status": "success",
            "message": f"{job_name} executed successfully",
            "logs": logs,
        }

    except subprocess.CalledProcessError as e:
        raise HTTPException(status_code=500, detail=f"Error executing script: {e}")


@apilog.post("/runscript")
async def runscript(job_name: str = Form(...)):
    root_dir = Path(__file__).resolve().parent.parent
    venv_dir = root_dir / "venv" / "Scripts" / "python.exe"
    script_path = root_dir / f"PushPull{job_name}.py"

    try:
        result = run(
            [venv_dir, script_path],
            stdout=PIPE,
            stderr=PIPE,
            text=True,
            cwd=str(root_dir),
        )

        if result.returncode != 0:
            raise HTTPException(
                status_code=500,
                detail=f"Error running script {job_name}: {result.stderr}",
            )

        logs = result.stdout.strip().splitlines()
        errors = []

        for line in logs:
            try:
                log_entry = json.loads(line)
                if log_entry.get("status") == "error":
                    errors.append(log_entry)
            except json.JSONDecodeError:
                continue

        if errors:
            raise HTTPException(
                status_code=400,
                detail=f"Script executed with errors: {errors}",
            )

        return {
            "status": "success",
            "message": f"PushPull for {job_name} executed successfully",
            "logs": logs,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error executing script: {e}")


@apilog.get("/log", response_model=ApiResponse)
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
        meta=Meta(timestamp=datetime.utcnow(), version="1.0"),
    )

    return response


@apilog.get("/consume")
async def consume_data():
    message = consume_api()
    if message:
        return {"status": "success", "message": message}
    else:
        raise HTTPException(
            status_code=204,
            detail=f"No message/data available in the queue",
        )


@apilog.get("/consumetest")
async def consume_data_local():
    message = consume_api_local()
    if message:
        return {"status": "success", "message": message}
    else:
        raise HTTPException(
            status_code=204, detail="No message/data available in the queue"
        )
