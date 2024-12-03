from sqlalchemy.orm import Session

from . import models


def get_logs(db: Session, skip: int = 0, limit: int = 100):
    return (
        db.query(models.Log)
        .order_by((models.Log.id.desc()))
        .offset(skip)
        .limit(limit)
        .all()
    )


def get_logs_filter(db: Session, keyword: str, skip: int = 0, limit: int = 100):
    return (
        db.query(models.Log)
        .filter(
            models.Log.job_name.ilike(f"%{keyword}%")
        )  # Using ilike for case-insensitive match
        .order_by((models.Log.id.desc()))
        .offset(skip)
        .limit(limit)
        .all()
    )
