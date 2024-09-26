import logging
import pandas as pd
from jinja2 import Template
from typing import Union, Dict
from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from pathlib import Path


app = FastAPI()

# Serve static files like CSS
app.mount("/static", StaticFiles(directory="static"), name="static")

# Path to the CSV file
FILE = Path("100k_sample.csv")

IMPORTANCE_SCORES: Dict[str, float] = {
    "orderingcustomername": 0.3,
    "orderingcustomerdetails": 0.2,
    "orderingbankraw": 0.5}


class Profiling:
    def __init__(self, filename: str) -> None:
        self.filename = filename
        self.full_path = Path(filename).absolute()

    def read_data(self, n: int) -> pd.DataFrame:
        data = pd.read_csv(self.full_path, nrows=n)
        return data

    @staticmethod
    def missing_values(df: pd.DataFrame) -> pd.DataFrame:
        return df.isnull().sum()

    @staticmethod
    def nan_prop_data(df: pd.DataFrame) -> float:
        return df.isna().sum().sum() / df.size * 100

    @staticmethod
    def completeness_score(df: pd.DataFrame) -> list:
        completeness = [
            f"column={col}: completeness score for missings: {((df[col].isnull().sum() / df[col].size) * 100):.2f}%" for
            col in df.columns]
        return completeness

    @staticmethod
    def missing_values_relativity(df: pd.DataFrame, constants: Union[Dict[str, int], None] = None) -> list:
        results = []
        for column in df.columns:
            completeness = (df[column].isna().sum() / df[column].size) * 100
            if column in constants:
                adjusted = f"Adjusted completeness score for {column} is: {constants[column] * completeness:.2f}%"
                results.append(adjusted)
            else:
                results.append(f"Completeness score for {column} is: {completeness:.2f}%")
        return results


profiling = Profiling("100k_sample.csv")


# Define the FastAPI routes

@app.get("/", response_class=HTMLResponse)
async def index():
    return """
    <html>
        <head>
            <title>Data Profiling</title>
            <link rel="stylesheet" href="/static/style.css">
        </head>
        <body>
            <h1>Data Profiling Tool</h1>
            <form action="/profile" method="post">
                <label for="n">Number of Rows to Load:</label>
                <input type="number" id="n" name="n" value="5" min="1">
                <button type="submit">Profile Data</button>
            </form>
        </body>
    </html>
    """


@app.post("/profile", response_class=HTMLResponse)
async def profile_data(n: int = Form(...)):
    # Read the specified number of rows
    raw_data = profiling.read_data(n=n)

    # Compute missing values and completeness score
    missing_values = profiling.missing_values(raw_data).to_dict()
    nan_prop = profiling.nan_prop_data(raw_data)
    completeness_scores = profiling.completeness_score(raw_data)
    relativity_scores = profiling.missing_values_relativity(raw_data, constants=IMPORTANCE_SCORES)

    # Render the results in the UI
    html_content = f"""
    <html>
        <head>
            <title>Data Profiling Results</title>
            <link rel="stylesheet" href="/static/style.css">
        </head>
        <body>
            <h1>Profiling Results for {n} rows</h1>
            <h2>Missing Values</h2>
            <pre>{missing_values}</pre>
            <h2>NaN Proportion in Data</h2>
            <p>{nan_prop:.2f}% of the data is missing.</p>
            <h2>Completeness Scores</h2>
            <ul>
                {''.join([f"<li>{score}</li>" for score in completeness_scores])}
            </ul>
            <h2>Missing Values Relativity</h2>
            <ul>
                {''.join([f"<li>{score}</li>" for score in relativity_scores])}
            </ul>
            <a href="/">Go Back</a>
        </body>
    </html>
    """
    return HTMLResponse(content=html_content)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)


