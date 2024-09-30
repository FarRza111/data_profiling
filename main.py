import uvicorn
import shutil
import logging
from datetime import datetime
from models import SQLALCHEMY_DATABASE_URL
from views import calculate_metrics
import pandas as pd
from pathlib import Path
from jinja2 import Template
from typing import Union, Dict
from fastapi import FastAPI, Request, Form, UploadFile, File
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from IPython.display import HTML

app = FastAPI()

# Serve static files like CSS
app.mount("/static", StaticFiles(directory="static"), name="static")
# Path to the CSV file
FILE = Path("100k_sample.csv")

IMPORTANCE_WEIGHTS: dict[str, float | int] = {
    "col1": 0.4,
    "col2": 0.4,
    "col3": 1

}

EXAMPLE_SCORES: dict[str, float | int] = {

    "col1": 0.4,
    "col2": 0.4,
    "col3": 1
}


class Profiling:
    def __init__(self, filename: str) -> None:
        self.filename = filename
        self.full_path = Path(filename).absolute()

    def read_data(self, n: int, usecols=None)-> pd.DataFrame:

        if usecols is None:
            usecols = [
                "col1", "col2", "col3"
            ]
        data = pd.read_csv(self.full_path, nrows=n, usecols=usecols)
        return data

    @staticmethod
    def calc_metrics(df, importance_scores: dict, constants: Union[Dict[str, int], None] = None):
        metrics = calculate_metrics(df)
        metrics_df = pd.DataFrame(metrics).T
        metrics_df['date'] = datetime.now().date()
        metrics_df.reset_index()
        final_df = metrics_df.reset_index().rename(columns={'index': 'column_names'})
        return final_df


    @staticmethod
    def missing_values(df: pd.DataFrame) -> pd.DataFrame:
        return df.isnull().sum()


    @staticmethod
    def nan_prop_data(df: pd.DataFrame) -> float:
        return df.isna().sum().sum() / df.size * 100

# profiling = Profiling("100k_sample.csv")

@app.get("/", response_class=HTMLResponse)
async def index():
    return """
    <html>
        <head>
            <link rel="stylesheet" href="/static/style.css">
        </head>
        <body>
            <h1>  </h1>
            <form action="/profile" method="post" enctype="multipart/form-data">
                <label for="file">Upload a CSV File:</label>
                <input type="file" id="file" name="file" accept=".csv" required>
                <label for="n">Number of Rows to Load:</label>
                <input type="number" id="n" name="n" value="5" min="1">
                <button type="submit">Profile Data</button>
            </form>
        </body>
    </html>
    """

@app.post("/profile", response_class=HTMLResponse)
async def profile_data(n: int = Form(...), file: UploadFile = File(...)):
    temp_file_path = Path("temp_uploaded_file.csv")
    with open(temp_file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    profiling = Profiling(str(temp_file_path))
    raw_data = profiling.read_data(n=n)
    missing_values = profiling.missing_values(raw_data).to_dict()
    nan_prop = profiling.nan_prop_data(raw_data)
    calculated_metrics = profiling.calc_metrics(raw_data, IMPORTANCE_WEIGHTS)

    # Render the results in the UI
    html_content = f"""
    <html>
        <head>
            <title>Data Profiling Results for table {profiling.filename}</title>       
            <link rel="stylesheet" href="/static/style.css">
        </head>
        <body>
            <div>
                 <h1 class="col-xs-8 text-center"> Profiling Results for {n} rows</h1>
            </div> 

            <h2> Missing Values:</h2>
            <pre>{missing_values}</pre>
            <h2>NaN Proportion in Data</h2>
             <p>  {nan_prop:.2f}% of the data is missing.</p>

            <h2> Metrics Matrix </h2>
            </ul>
                {calculated_metrics.to_html()}
            </ul>

            <a href="/">Go Back</a>
        </body>
    </html>
    """
    return HTMLResponse(content=html_content)
