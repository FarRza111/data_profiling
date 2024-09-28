import pandas as pd
from views import *
from datetime import date
from pydantic import BaseModel
from datetime import datetime
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Float, DateTime

# SQLAlchemy Database URL (using SQLite for this example)
SQLALCHEMY_DATABASE_URL = "sqlite:///./test.db"

# Create the engine and session
engine = create_engine(SQLALCHEMY_DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Base model
Base = declarative_base()

# Model definition
class MetricsHistory(Base):
    __tablename__ = "metrics_history"

    id = Column(Integer, primary_key=True, index=True)
    column_name = Column(String, index=True)
    adjusted_completeness = Column(Float, index=True, default=None)
    completeness_score = Column(Float)
    weighted_completeness = Column(Float)
    accuracy_score = Column(Float)
    error_rate = Column(Float)
    uniqueness_score = Column(Float)
    outliers_count = Column(Integer)
    date = Column(DateTime, default=datetime.utcnow)

class MetricCreate(BaseModel):
    column_name: str
    completeness_score: float
    weighted_completeness: float
    accuracy_score: float
    error_rate: float
    uniqueness_score: float
    outliers_count: int
    adjusted_completeness: float
    date: datetime


class Metric(MetricCreate):
    id: int

    class Config:
        orm_mode = True


# Function to create a metric
def create_metric(db: Session, metric: MetricCreate):
    db_metric = MetricsHistory(**metric.dict())  # Converts the Pydantic model to SQLAlchemy model
    db.add(db_metric)
    db.commit()  # Saves the changes in the database
    db.refresh(db_metric)  # Updates the instance with the latest data from the DB
    return db_metric


# Dependency for session (Context manager for handling the session)
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def ingest_dataframe(df: pd.DataFrame, db: Session):

    for index, row in df.iterrows():
        metric_data = MetricCreate(
            column_name=row['column_name'],
            completeness_score=row['completeness_score'],
            weighted_completeness=row['weighted_completeness'],
            adjusted_completeness=row['adjusted_completeness'],
            accuracy_score=row['accuracy_score'],
            error_rate=row['error_rate'],
            uniqueness_score=row['uniqueness_score'],
            outliers_count=row['outliers_count'],
            date=row['date']  # Ensure the correct datetime format
        )
        # Insert each row into the database
        create_metric(db=db, metric=metric_data)


if __name__ == "__main__":
    df = pd.read_csv(r"{your_path}"),
    nrows=10,
         usecols=["date",
                  "amounta",
                  "orderdet",
                  "customername",
                  "pop_date",
                  "deletion_date",
                  "banknames"]
            )

    metrics = calculate_metrics(df, importance_weights)
    metrics_df = pd.DataFrame(metrics).T
    metrics_df['date'] = datetime.now().date()
    metrics_df.reset_index()

    df = metrics_df.reset_index().rename(columns={'index': 'column_name'})
    Base.metadata.create_all(bind=engine)

    db = SessionLocal()

    try:
        ingest_dataframe(df,db)
    finally:
        db.close()

