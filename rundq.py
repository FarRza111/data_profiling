import os
import logging
import datetime
import pandas as pd
import pyodbc as odbc


# master_metrics.sql
#  WITH T AS (
#            SELECT
#                 'validity' AS metric_type,
#                 'customername' AS cde,
#             COUNT(*) AS total_records,
#             SUM(CASE WHEN LENGTH(customer_id) = 11 AND regexp_like(customer_id, '^[A-Z]4[A-Z]2[A-Z0-9]2[A-Z0-9]3$') THEN 1 ELSE 0 END) as valid_records
#             FROM table_name
#             UNION ALL

#              SELECT
#                 'completeness AS metric_type,
#                 'customer_id' AS cde,
#             COUNT(*) AS total_records,
#             SUM(CASE WHEN customer_id IS NOT NULL THEN 1 ELSE 0 END) as valid_records
#             FROM table_name

#             )

# SELECT
#  current_database() as db_name, to_date(now()) as run_date,  "sales_fact" as table_name, *, total_records - valid_records as invalid_records, valid_records/total_records as metric_percentage
# FROM T








# reading master queries from master_metrics.sql
def read_sql_file(file_name):
    with open(file_name, 'r') as file:
        return file.read()


path = os.getcwd()
master_query = os.path.join(path, "master_metrics.sql")
master_metrics = read_sql_file(master_query)
connection = odbc.connect('DSN=impala', autocommit=True)
data = pd.read_sql(master_metrics, connection)

logging.getLogger().setLevel(logging.INFO)

try:
    conn = odbc.connect('DSN=impala', autocommit=True)
    cursor = conn.cursor()
    today = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

    cursor.execute("SELECT COUNT(*) FROM sales_db.master_metrics WHERE run_date = ?", (today,))
    existing_records = cursor.fetchone()[0]

    if existing_records > 0:  # used to check if already exists data there
        print(f"Today's data already exists in the table. No new records inserted on {datetime.datetime.now()}")
    else:
        column_names = ", ".join(data.columns)
        value_placeholder = ", ".join(["?"] * len(data.columns))

        insert_query = f"INSERT INTO sales_db.master_metrics ({column_names}) VALUES ({value_placeholder})"

        values = [tuple(row.values) for _, row in data.iterrows()]  # data should be fetch first

        # print(insert_query)  # For debugging
        # print(values)  # For debugging
        cursor.executemany(insert_query, values)

        # Commit changes
        conn.commit()

        message = f"Rule table updated with {len(values)} records on {datetime.datetime.now()}"
        logging.info(message)
        print(message)

except Exception as e:
    print(f"An error occurred: {e}")
finally:
    # Connection must be closed in the end
    if conn:
        conn.close()



