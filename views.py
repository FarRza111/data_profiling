import pandas as pd
from datetime import datetime
from typing import Union, Dict
import numpy as np

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

def calculate_metrics(df, importance_weights: Dict[str, Union[int, float]] = IMPORTANCE_WEIGHTS,
                      example_scores: dict[str, Union[int, float]] = EXAMPLE_SCORES):
    results = {}

    for col in df.columns:
        total_count = len(df[col])  # Total number of rows
        missing_count = df[col].isna().sum()  # Count NaN values for real missing data
        completeness_score = (1 - (missing_count / total_count)) * 100 if total_count > 0 else 0
        adjusted_completeness = (df[col].isna().sum() / len(df[col])) * 100 * EXAMPLE_SCORES[ col] if completeness_score < 95 else completeness_score
        uniqueness_score = (df[
                                col].nunique() / total_count) * 100 if total_count > 0 else 0  # Uniqueness: Percentage of unique values in the column
        weighted_completeness = completeness_score * IMPORTANCE_WEIGHTS.get(col, 1)
        # weighted_missing_on_actual_data = total_count * importance_weights[col]/()

        if pd.api.types.is_numeric_dtype(df[col]):
            accuracy_score = (df[col] > 0).sum() / total_count * 100
            Q1 = df[col].quantile(0.25)
            Q3 = df[col].quantile(0.75)
            IQR = Q3 - Q1
            outliers = ((df[col] < (Q1 - 1.5 * IQR)) | (df[col] > (Q3 + 1.5 * IQR))).sum()

        else:
            # Assume 100% accuracy for text fields (can be refined based on our rules and controls need to discuss with stream3)
            accuracy_score = 60
            outliers = 0  # No outlier detection for text fields

        error_rate = 100 - accuracy_score

        results[col] = {
            'total_count': total_count,
            'missing_count': missing_count,
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
                     nrows=10
                     , usecols=["col1", "col2","col3"]

                     )

    metrics = calculate_metrics(df, IMPORTANCE_WEIGHTS)
    metrics_df = pd.DataFrame(metrics).T
    metrics_df['date'] = datetime.now().date()
    metrics_df.reset_index()

    final_df = metrics_df.reset_index().rename(columns={'index': 'column_names'})

    print(final_df)
