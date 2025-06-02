from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Define a Python function to print a message
def print_hello_message():
    print("Hello from Airflow! This is a simple message.")

with DAG(
    dag_id="simple_message_dag",
    start_date=pendulum.datetime(2025, 6, 1, tz="UTC"),
    schedule=None,  # Set to None for a manually triggered DAG
    catchup=False,  # Do not backfill for past missed schedules
    tags=["example", "message"],
) as dag:
    # Task 1: Print a message using BashOperator
    # This task executes a shell command to print a message.
    bash_task = BashOperator(
        task_id="print_message_bash",
        bash_command="echo 'This message is from the BashOperator!'",
    )

    # Task 2: Print a message using PythonOperator
    # This task executes a Python callable (the print_hello_message function).
    python_task = PythonOperator(
        task_id="print_message_python",
        python_callable=print_hello_message,
    )

    # Define the task dependencies
    # The 'bash_task' will run first, and then 'python_task' will run.
    
    bash_task >> python_task
