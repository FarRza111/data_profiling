import streamlit as st
import pandas as pd
# import numpy as np
# import plotly.express as px
from io import StringIO
from datetime import datetime
import os



def calculate_metrics(column_data:str) -> None:
    total = len(column_data)
    missing = column_data.isna().sum()
    non_missing = total - missing
    unique = column_data.nunique()

    completeness = non_missing / total if total > 0 else 0       # Completeness score: Proportion of non-missing values
    quality = unique / non_missing if non_missing > 0 else 0     # Quality score: Proportion of unique non-missing values
    adjusted_completeness = completeness * 0.9  # Simple example adjustment &

    outliers_count = 0
    if pd.api.types.is_numeric_dtype(column_data):
        q1 = column_data.quantile(0.25)
        q3 = column_data.quantile(0.75)
        iqr = q3 - q1
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr
        outliers_count = ((column_data < lower_bound) | (column_data > upper_bound)).sum()

    return completeness, quality, adjusted_completeness, outliers_count, missing


# Title of the UI
st.title("Data Quality Dashboard")

# CSV file Upload
uploaded_file = st.file_uploader("Choose a file", type="csv")

if uploaded_file is not None:

    file = StringIO(uploaded_file.getvalue().decode("utf-8"))
    df = pd.read_csv(file)

    num_rows = st.slider("Select number of rows to display", 1, min(len(df), 100),
                         5) # Previous of data
    st.write("Data Preview (showing first {} rows):".format(num_rows), df.head(num_rows))

    # column selections for checking
    selected_columns = st.multiselect("Select Columns to Run Checks", df.columns)

    # Storage of run results
    historical_storage_file = "data_quality_history.csv"

    if st.button("Run Data Quality Checks"):
        if not selected_columns:
            st.error("select at least one column to run checks !!!")
        else:
            results = []
            for column in selected_columns:
                column_data = df[column]
                completeness, quality, adjusted_completeness, outliers_count, missing = calculate_metrics(column_data)

                result = {
                    "timestamp": datetime.now(),
                    "column": column,
                    "completeness": completeness,
                    "quality_score": quality,
                    "adjusted_completeness": adjusted_completeness,
                    "outliers_count": outliers_count,
                    "missing_values_percentage": (missing / len(column_data)) * 100,
                    "data_type": column_data.dtype,  # Data type
                    # "mean": column_data.mean() if pd.api.types.is_numeric_dtype(column_data) else None,
                    # "median": column_data.median() if pd.api.types.is_numeric_dtype(column_data) else None,
                    # "std_dev": column_data.std() if pd.api.types.is_numeric_dtype(column_data) else None,
                }

                results.append(result)

            results_df = pd.DataFrame(results)
            st.write("Data Quality Results:", results_df)

            if os.path.exists(historical_storage_file):
                historical_df = pd.read_csv(historical_storage_file)
                historical_df = pd.concat([historical_df, results_df], ignore_index=True)
            else:
                historical_df = results_df

            historical_df.to_csv(historical_storage_file, index=False)

            # Button to download results
            csv = results_df.to_csv(index=False)
            st.download_button("Download Results as CSV", csv, "data_quality_results.csv")

            if st.button("Show Historical Trends"):
                if os.path.exists(historical_storage_file):
                    historical_df['timestamp'] = pd.to_datetime(historical_df['timestamp'])
                    historical_df.set_index('timestamp', inplace=True)

                    # Plot completeness over time for selected columns
                    for column in selected_columns:
                        if column in historical_df.columns:
                            fig = px.line(
                                historical_df,
                                x=historical_df.index,
                                y='completeness',
                                title=f"Completeness Trend for {column}",
                                labels={'completeness': 'Completeness'},
                                markers=True
                            )
                            st.plotly_chart(fig)
                else:
                    st.warning("No historical data available.")

