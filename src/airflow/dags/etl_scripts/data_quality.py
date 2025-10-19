# -*- coding: utf-8 -*-
import asyncio
import io
from typing import List, Optional

import aioboto3
import pandas as pd
from airflow.config import config
from utils.dependencies import get_airflow_s3_handler
from utils.logging import setup_logging
from utils.s3_service import S3Handler

logger = setup_logging(log_level=config.log_level)

def get_parquet_filenames(s3_handler: S3Handler) -> List[str]:
    """
    Gets a list of all .parquet file names from the origin bucket.

    Uses a paginator to handle buckets with a large number of objects.

    Args:
        s3_handler (S3Handler): An initialized S3 handler configured for the
            origin bucket.

    Returns:
        List[str]: A list of S3 object keys ending with '.parquet'.
    """
    paginator = s3_handler.s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(
        Bucket=config.bucket_destiny_name, Prefix=config.full_destiny_data_prefix
    )

    return [
        obj['Key'] for page in pages
        for obj in page.get('Contents', [])
        if obj['Key'].endswith('.parquet')
    ]

async def process_files_concurrently(
    aio_session: aioboto3.Session, parquet_list: List[str]
) -> List[pd.DataFrame]:
    """
    Orchestrates concurrent downloading and processing of parquet files.

    Args:
        aio_session (aioboto3.Session): The aioboto3 session to be used by tasks.
        parquet_list (List[str]): The list of S3 object keys to process.

    Returns:
        List[pd.DataFrame]: A list of valid DataFrames, excluding any that failed
            processing or validation.
    """
    tasks = [process_parquet_key_async(aio_session, key) for key in parquet_list]
    results = await asyncio.gather(*tasks)
    return [df for df in results if df is not None]

async def process_parquet_key_async(
    session: aioboto3.Session, key: str
) -> Optional[pd.DataFrame]:
    """
    Asynchronously downloads a parquet file and reads it into a DataFrame.

    Args:
        session (aioboto3.Session): The aioboto3 session for creating a client.
        key (str): The S3 object key of the parquet file to process.

    Returns:
        Optional[pd.DataFrame]: A DataFrame if the file is read successfully, otherwise None.
    """
    logger.debug(f"Processing {key}...")
    try:
        async with session.client("s3") as s3_client:
            response = await s3_client.get_object(
                Bucket=config.bucket_destiny_name, Key=key
            )
            body = await response['Body'].read()
            buffer = io.BytesIO(body)
            df = pd.read_parquet(buffer)
            
        return df
        
    except Exception as e:
        logger.error(f"A critical error occurred while processing {key}: {e}")
        return None
    
def check_nulls(df: pd.DataFrame) -> None:
    if df.isnull().any().any():
        logger.warning("There were found rows with null values.")
    else:
        logger.info("Null test passed with success.")

def check_quantity(df: pd.DataFrame) -> None:
    if not (df['quantity'] > 0).all():
        logger.warning("Column *quantity* has values below 0.")
    else:
        logger.info("Positve *quantity* test passed with success.")
    
def main():
    logger.info("Initializing S3 handler for DESTINATION bucket...")
    dest_s3_handler = get_airflow_s3_handler(
        bucket_name=config.bucket_destiny_name
    )

    parquet_list = get_parquet_filenames(s3_handler=dest_s3_handler)
    if not parquet_list:
        logger.info("No parquet files found to process.")
        return
    
    valid_dfs = asyncio.run(
        process_files_concurrently(dest_s3_handler.s3_aio_client, parquet_list)
    )

    if not valid_dfs:
        logger.info("No valid DataFrames were created. Exiting.")
        return
    
    final_df = pd.concat(valid_dfs)

    # To add new validations, we simply need to develop the function that makes that validation and put it on this list.
    validations = [
        check_nulls,
        check_quantity
    ]
    
    for function in validations:
        function(final_df)
    