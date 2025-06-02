from __future__ import annotations

import logging
import os

from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

# --- Configuration Variables ---
S3_CONN_ID = "aws_default"  # Your Airflow S3 connection ID
POSTGRES_CONN_ID = "postgres_default"  # Your Airflow PostgreSQL connection ID
S3_BUCKET_NAME = "piyush-airflow"  # Replace with your S3 bucket name
S3_KEY = "data/geographic-units-by-industry-and-statistical-area-2000-2024-descending-order-february-2024.csv"  # IMPORTANT: Ensure your CSV is at this path in S3!
POSTGRES_TABLE_NAME = "anzsic_data"  # Name of the table in PostgreSQL

# Define your table schema based on your CSV file columns
# Ensure these match the order and data types in your CSV
TABLE_SCHEMA = f"""
    CREATE TABLE IF NOT EXISTS {POSTGRES_TABLE_NAME} (
        record_id SERIAL PRIMARY KEY,
        anzsic_code VARCHAR(100),
        area_code VARCHAR(100),
        observation_year INTEGER,
        geographic_count INTEGER,
        economic_count INTEGER,
        ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
"""

# Headers from your CSV (must match order and number of columns you want to copy)
CSV_HEADERS = [
    "anzsic06", "Area", "year", "geo_count", "ec_count"
]
# Corresponding database column names for the COPY FROM statement
# Make sure this order aligns with CSV_HEADERS and the CREATE TABLE statement
DB_COLUMNS = [
    "anzsic_code", "area_code", "observation_year", "geographic_count", "economic_count"
]

log = logging.getLogger(__name__)

@dag(
    dag_id="s3_to_rds_anzsic_stream_dag",
    start_date=datetime(2025, 6, 2),
    schedule=None, # Set a schedule if you want it to run periodically
    catchup=False,
    tags=["s3", "postgres", "aws", "data_ingestion", "anzsic", "streaming"],
    doc_md="""
    ### S3 to AWS RDS PostgreSQL Data Ingestion DAG (ANZSIC Data - Streamed)

    This DAG demonstrates how to:
    1. Create a PostgreSQL table for ANZSIC data if it doesn't exist.
    2. Directly stream a CSV file from an S3 bucket into the PostgreSQL table
       using `COPY FROM STDIN` via `PostgresHook.copy_expert()`,
       **without writing the file to the Airflow worker's local filesystem.**

    This method is more efficient and avoids potential `FileNotFoundError` issues.

    **Pre-requisites:**
    - Airflow connections `aws_default` (S3) and `postgres_default` (PostgreSQL) configured.
    - An S3 bucket (`your-s3-bucket-name`) with a CSV file (`data/anzsic_data.csv`).
    - The CSV headers must match the `CSV_HEADERS` list, and the data types must be compatible with `TABLE_SCHEMA`.
    """
)
def s3_to_rds_anzsic_stream_dag():

    @task
    def stream_s3_to_postgres(s3_bucket: str, s3_key: str, table_name: str, db_columns: list[str]):
        """
        Streams data directly from an S3 CSV file into a PostgreSQL table
        using COPY FROM STDIN, without saving the file locally.
        """
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = None # Initialize conn to None

        log.info(f"Attempting to stream s3://{s3_bucket}/{s3_key} to {table_name}...")

        try:
            # Get the S3 object as a file-like object
            # The get_key method returns an S3 object (boto3.s3.Object)
            s3_object = s3_hook.get_key(key=s3_key, bucket_name=s3_bucket)
            
            # Get the body of the S3 object, which is a StreamingBody
            # This acts like a file-like object that can be read
            s3_file_stream = s3_object.get()['Body']

            conn = pg_hook.get_conn()
            cursor = conn.cursor()

            # Construct the COPY FROM STDIN command
            copy_sql = f"""
                COPY {table_name} ({', '.join(db_columns)})
                FROM STDIN WITH (FORMAT CSV, HEADER TRUE)
            """
            log.info(f"Executing COPY command: {copy_sql}")

            # Use copy_expert to directly stream data from S3 to PostgreSQL
            # The s3_file_stream (StreamingBody) behaves like a file object
            cursor.copy_expert(copy_sql, s3_file_stream)
            conn.commit()
            log.info(f"Data successfully streamed from S3 to {table_name}")

        except Exception as e:
            log.error(f"Error streaming data from S3 to PostgreSQL: {e}")
            if conn:
                conn.rollback() # Rollback on error
            raise # Re-raise the exception to fail the task
        finally:
            if conn:
                conn.close()
            # The s3_file_stream should be automatically closed when the task finishes
            # or when the 'with' block (if used) exits. No explicit close needed for StreamingBody here.


    create_table = PostgresOperator(
        task_id="create_anzsic_table",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=TABLE_SCHEMA,
    )

    # Call the combined streaming task
    stream_data_task = stream_s3_to_postgres(
        s3_bucket=S3_BUCKET_NAME,
        s3_key=S3_KEY,
        table_name=POSTGRES_TABLE_NAME,
        db_columns=DB_COLUMNS
    )

    # Define task dependencies
    chain(
        create_table,
        stream_data_task
    )

s3_to_rds_anzsic_stream_dag()