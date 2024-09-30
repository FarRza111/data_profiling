import pandas as pd
from fastapi import FastAPI, Depends
from sqlalchemy.orm import Session
from views import *
from datetime import datetime
from pydantic import BaseModel
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Float, DateTime
import plotly.graph_objs as go
from fastapi.responses import HTMLResponse
import pandas as pd
import numpy as np
from datetime import datetime, timedelta



# SQLAlchemy Database URL
SQLALCHEMY_DATABASE_URL = "sqlite:///./test.db"

# Create the engine and session
engine = create_engine(SQLALCHEMY_DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Base model
Base = declarative_base()

# FastAPI application
app = FastAPI()



IMPORTANCE_WEIGHTS: dict[str, float | int] = {

    "col1": 0.6,
    "col2": 0.6,
    "col3": 0.9,
}

EXAMPLE_SCORES: dict[str, float | int] = {

    "col1": 0.4,
    "col2": 0.4,
    "col3": 1,
}


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


# Function to ingest the dataframe into the database
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
            date=row['date']
        )
        create_metric(db=db, metric=metric_data)


# Visualization route
@app.get("/visualization", response_class=HTMLResponse)
def get_visualization(db: Session = Depends(get_db)):
    # Query the data
    metrics = db.query(MetricsHistory.date, MetricsHistory.completeness_score, MetricsHistory.outliers_count).all()

    # Convert to DataFrame for easier plotting
    df = pd.DataFrame(metrics, columns=['date', 'completeness_score', 'outliers_count'])

    # Create a Plotly line chart for completeness score and outliers count
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df['date'], y=df['completeness_score'], mode='lines', name='Completeness Score'))
    fig.add_trace(go.Scatter(x=df['date'], y=df['outliers_count'], mode='lines', name='Outliers Count', yaxis='y2'))

    # Add dual y-axis
    fig.update_layout(
        title="Completeness Score and Outliers Count Over Time",
        xaxis_title="Date",
        yaxis_title="Completeness Score",
        yaxis2=dict(
            title="Outliers Count",
            overlaying='y',
            side='right'
        )
    )

    # Return the HTML representation of the plot
    return HTMLResponse(content=fig.to_html(full_html=False))


# Ingest data example
if __name__ == "__main__":

    # Number of rows in the simulated DataFrame
    n_rows = 10

    # Simulate a DataFrame with test data
    np.random.seed(42)  # For reproducibility

    data = {
        "column_name": [f"column_{i}" for i in range(n_rows)],
        "completeness_score": np.random.uniform(0.7, 1.0, n_rows),  # Random completeness between 0.7 and 1.0
        "weighted_completeness": np.random.uniform(0.6, 0.9, n_rows),
        # Random weighted completeness between 0.6 and 0.9
        "accuracy_score": np.random.uniform(0.8, 1.0, n_rows),  # Random accuracy between 0.8 and 1.0
        "error_rate": np.random.uniform(0.0, 0.2, n_rows),  # Random error rate between 0.0 and 0.2
        "uniqueness_score": np.random.uniform(0.8, 1.0, n_rows),  # Random uniqueness between 0.8 and 1.0
        "outliers_count": np.random.randint(0, 10, n_rows),  # Random integer outliers count between 0 and 10
        "adjusted_completeness": np.random.uniform(0.7, 1.0, n_rows),
        # Random adjusted completeness between 0.7 and 1.0
        "date": [datetime.now() - timedelta(days=i) for i in range(n_rows)]  # Date going back from today
    }

    # Create the DataFrame
    df = pd.DataFrame(data)

    # df = pd.read_csv(
    #     r"{your_path}",
    #     nrows=10,
    #     usecols=["date", "amounta", "orderdet", "customername", "pop_date", "deletion_date", "banknames"]
    # )

    metrics = calculate_metrics(df, IMPORTANCE_WEIGHTS)
    metrics_df = pd.DataFrame(metrics).T
    metrics_df['date'] = datetime.now().date()
    metrics_df.reset_index()

    df = metrics_df.reset_index().rename(columns={'index': 'column_name'})
    Base.metadata.create_all(bind=engine)

    db = SessionLocal()

    try:
        ingest_dataframe(df, db)
    finally:
        db.close()

