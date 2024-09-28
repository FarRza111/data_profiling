import pandas as pd
from datetime import datetime
import numpy as np


importance_weights = {

        "col1": 0.4,
        "col2": 0.4,
        "col3": 1,
        "col4": 2,
        "col5": 0.7,
        "col6":0.2,
        "col7": 0.9
}


def calculate_metrics(df, importance_weights):

    results = {}
    example_socres = {

     "col1": 0.1,
        "col2": 0.1,
        "col3": 1,
        "col4": 0.9,
        "col5": 0.6,
        "col6":0.1,
        "col7": 0.8


    }

    for col in df.columns:
        total_count = len(df[col])  # Total number of rows
        missing_count = df[col].isna().sum()  # Count NaN values for real missing data
        completeness_score = (1 - (missing_count / total_count)) * 100 if total_count > 0 else 0
        adjusted_completeness = (df[col].isna().sum()/len(df[col])) * 100 * example_socres[col]
        uniqueness_score = (df[col].nunique() / total_count) * 100 if total_count > 0 else 0         # Uniqueness: Percentage of unique values in the column
        weighted_completeness = completeness_score * importance_weights.get(col, 1)
        # weighted_missing_on_actual_data = total_count * importance_weights[col]/()

        if col == 'amount32a':
            accuracy_score = (df[col] > 0).sum() / total_count * 100
            Q1 = df[col].quantile(0.25)
            Q3 = df[col].quantile(0.75)
            IQR = Q3 - Q1
            outliers = ((df[col] < (Q1 - 1.5 * IQR)) | (df[col] > (Q3 + 1.5 * IQR))).sum()

        else:
            # Assume 100% accuracy for text fields (can be refined based on our rules and controls need to discuss with stream3)
            accuracy_score = 60
            outliers = 0  # No outlier detection for text fields

        # Error Rate: Complement of accuracy
        error_rate = 100 - accuracy_score

        results[col] = {
            'completeness_score': completeness_score,
            'weighted_completeness': weighted_completeness,
            'accuracy_score': accuracy_score,
            'error_rate': error_rate,
            'uniqueness_score': uniqueness_score,
            'outliers_count': outliers,  # Add outlier count to the metrics
            'adjusted_completeness': adjusted_completeness
        }

    return results


if __name__ == "__main__":
    df = pd.read_csv('100k_sample.csv',
                     nrows=10,
                     usecols=[
                  "col1": 0.1,
                  "col2": 0.1,
                  "col3": 1,
                  "col4": 0.9,
                  "col5": 0.6,
                  "col6":0.1,
                  "col7": 0.8
                     ]
                     )

    metrics = calculate_metrics(df, importance_weights)
    metrics_df = pd.DataFrame(metrics).T
    metrics_df['date'] = datetime.now().date()
    metrics_df.reset_index()

    final_df = metrics_df.reset_index().rename(columns={'index': 'column_names'})
    # final_df.to_csv("history_quality.csv", index=False)
    # csv_history_file = "history_quality.csv"

    # try:
    #     history_df = pd.read_csv(csv_history_file)
    #     history_df = pd.concat([history_df, final_df], ignore_index=True)
    # except FileNotFoundError:
    #     history_df = final_df
    #
    # history_df.to_csv("history_quality.csv", index=False)
    # print(history_df)
    # print(final_df)

    # for index, row in final_df.iterrows():
    #     # metric_data = (
    #     #
    #     #     row['adjusted_completeness'],
    #     #
    #     # )
    #     #
    #     # print(metric_data)
    #     print(row['column_names'])

    final_df
