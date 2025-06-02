from __future__ import annotations

import logging
import os

from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
# S3Hook and PostgresHook imports are no longer needed if not used in other tasks
# from airflow.providers.amazon.aws.hooks.s3 import S3Hook
# from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from datetime import datetime


# --- Configuration Variables ---
S3_CONN_ID = "aws_default"  # Your Airflow S3 connection ID (used by GlueOperator for AWS creds implicitly)
POSTGRES_CONN_ID = "postgres_default"  # Your Airflow PostgreSQL connection ID (only for creating staging table)
S3_BUCKET_NAME = "piyush-airflow"  # Replace with your S3 bucket name
S3_KEY = "data/geographic-units-by-industry-and-statistical-area-2000-2024-descending-order-february-2024.csv"  # IMPORTANT: Ensure your CSV is at this path in S3!
S3_INPUT_PATH = f"s3://{S3_BUCKET_NAME}/{S3_KEY}" # Full S3 path for Glue
POSTGRES_STAGING_TABLE_NAME = "anzsic_staging" # New staging table for transformed data
GLUE_JOB_NAME = "transform-etl" # Name of your Glue job
GLUE_ROLE_ARN = "arn:aws:iam::123456789:role/service-role/AWSGlueServiceRole" # Replace with your Glue job's IAM role ARN
GLUE_CONNECTION_NAME = "Postgresql connection" # The exact name of your Glue JDBC connection
DATABASE_NAME="airflowdb"

S3_ETL_SCRIPT_KEY = "etl-jobs/transform_job.py"
GLUE_SCRIPT_S3_PATH=f"s3://{S3_BUCKET_NAME}/{S3_ETL_SCRIPT_KEY}"

# Define schema for the STAGING table which will include the new column
# This schema must precisely match the columns that Glue will write.
STAGING_TABLE_SCHEMA = f"""
    CREATE TABLE IF NOT EXISTS {POSTGRES_STAGING_TABLE_NAME} (
        anzsic_code VARCHAR(100),
        area_code VARCHAR(100),
        observation_year INTEGER,
        geographic_count INTEGER,
        economic_count INTEGER,
        ingestion_timestamp TIMESTAMP,
        area_year_combined VARCHAR(255)
    );
"""
# Removed record_id SERIAL PRIMARY KEY as Glue won't manage it if we only select specific columns.
# If you need a primary key, you'd need to handle it in Glue or add it after ingestion.
# For simplicity in this example, we're assuming the transformed table is a flat staging table.
# If you want a primary key, you might need to add it as a separate step or modify the Glue job
# to generate it if it's not present in the S3 data.

log = logging.getLogger(__name__)

@dag(
    dag_id="s3_transform_etl_rds_dag", # Changed DAG ID again
    start_date=datetime(2025, 6, 2),
    schedule=None,
    catchup=False,
    tags=["s3", "postgres", "aws", "data_ingestion", "anzsic", "glue", "etl"],
    doc_md="""
    ### S3 Direct to AWS RDS PostgreSQL Data ETL DAG (ANZSIC Data - Glue Transform)

    This DAG demonstrates how to:
    1. Create a PostgreSQL staging table for transformed ANZSIC data if it doesn't exist.
    2. Trigger an AWS Glue job to directly read a CSV file from an S3 bucket,
       add a new column by concatenating 'Area' and 'Year',
       and store the transformed data into the staging table in PostgreSQL.

    **Pre-requisites:**
    - Airflow connection `aws_default` (for GlueOperator) and `postgres_default` (for PostgresOperator) configured.
    - An S3 bucket (`your-s3-bucket-name`) with a CSV file (`data/anzsic_data.csv`).
    - An AWS Glue job (`anzsic-area-year-transformer`) configured with the provided script
      and an IAM role with necessary permissions, including S3 read access, and
      access to your PostgreSQL database via a Glue JDBC connection.
    - The S3 CSV headers should match the schema defined in the Glue job.
    """
)
def s3_transform_etl_rds_dag():

    # Task to create the staging table in PostgreSQL
    create_staging_table = PostgresOperator(
        task_id="create_anzsic_staging_table",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=STAGING_TABLE_SCHEMA,
    )
    log.info(f"GLUE_ROLE_ARN: {GLUE_ROLE_ARN}")
    # Task to trigger the AWS Glue ETL job
    trigger_glue_etl_job = GlueJobOperator(
        task_id="trigger_anzsic_glue_etl",
        job_name=GLUE_JOB_NAME,
        script_args={ # These parameters are passed to your Glue job script
            "--S3_INPUT_PATH": S3_INPUT_PATH, # Pass the S3 input path directly
            "--DATABASE_NAME": DATABASE_NAME, # Replace with your actual database name (e.g., 'postgres')
            "--TARGET_TABLE_NAME": POSTGRES_STAGING_TABLE_NAME,
            "--GLUE_CONNECTION_NAME": GLUE_CONNECTION_NAME
        },
        iam_role_arn =GLUE_ROLE_ARN,
        create_job_kwargs={
            "Role": GLUE_ROLE_ARN,
            "GlueVersion": "4.0",
            "WorkerType": "G.1X",
            "NumberOfWorkers": 2, # Initial number of workers (can scale up to MaxCapacity)
            "Connections": {"Connections": [GLUE_CONNECTION_NAME]},
            "Command": {
                "Name": "glueetl", # Or "pythonshell" if it's a Python Shell job
                "ScriptLocation": GLUE_SCRIPT_S3_PATH,
                "PythonVersion": "3" # Or "3.9", "3.10", etc., matching your Glue job config
            },
            "ExecutionClass": "FLEX", # Enable Flex execution
            "MaxCapacity": 5 # Maximum DPUs/Workers for auto-scaling. Adjust as needed."
        },
        wait_for_completion=True,
        deferrable=False
    )

    # Define task dependencies
    chain(
        create_staging_table,
        trigger_glue_etl_job
    )

s3_transform_etl_rds_dag()