# Save this file as e.g., dags/s3_processing_pipeline.py

from __future__ import annotations

import pendulum
from airflow.decorators import dag, task

from etl_scripts.etl import main as process_jsonl_main
from etl_scripts.data_quality import main as validate_parquet_main

@dag(
    dag_id='s3_jsonl_to_parquet_pipeline',
    # Runs every 30 minutes.
    schedule='*/30 * * * *',
    start_date=pendulum.datetime(2025, 10, 19, tz="UTC"),
    catchup=False,
    # Ensures a new run won't start if the previous one is still active.
    max_active_runs=1,
    doc_md="""
    ### S3 JSONL to Parquet ETL Pipeline
    This DAG processes raw JSONL files from an S3 bucket, consolidates them 
    into a Parquet file in another bucket, and then runs data quality checks 
    on the resulting file.
    """,
    tags=['etl', 's3', 'pandas', 'async'],
)

def s3_etl_dag():
    """
    This DAG defines the two main tasks of the pipeline:
    1.  **`process_raw_data`**: Corresponds to your first module. It fetches, transforms, and loads the data.
    2.  **`validate_processed_data`**: Corresponds to your second module. It runs data quality checks.
    """

    @task
    def process_raw_data():
        """
        Task to process raw JSONL files from S3, consolidate them into a 
        single Parquet file, upload it, and clean up the source files.
        """
        print("Starting raw data processing from JSONL files...")
        process_jsonl_main()
        print("Raw data processing complete.")

    @task
    def validate_processed_data():
        """
        Task to validate the consolidated Parquet file by running data 
        quality checks.
        """
        print("Starting data validation on the Parquet file...")
        validate_parquet_main()
        print("Data validation complete.")

    process_raw_data() >> validate_processed_data()
    
dag = s3_etl_dag()