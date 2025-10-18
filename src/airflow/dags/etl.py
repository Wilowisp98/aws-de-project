# -*- coding: utf-8 -*-
import asyncio
import io
from datetime import datetime
from typing import List, Optional
import ast
import json

import aioboto3
import pandas as pd
from airflow.dags.config import config
from utils.dependencies import create_s3_handler
from utils.logging import setup_logging
from utils.s3_service import S3Handler

logger = setup_logging(log_level=config.log_level)

DATE_COLUMNS = ['datetime']
SCHEMA = {
    'store_id': 'Int64',
    'transaction_id': 'Int64',
    'product_id': 'Int64',
    'quantity': 'Int64',
}


def get_json_filenames(s3_handler: S3Handler) -> List[str]:
    """
    Gets a list of all .json file names from the origin bucket.

    Uses a paginator to handle buckets with a large number of objects.

    Args:
        s3_handler (S3Handler): An initialized S3 handler configured for the
            origin bucket.

    Returns:
        List[str]: A list of S3 object keys ending with '.jsonl'.
    """
    paginator = s3_handler.s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(
        Bucket=config.bucket_origin_name, Prefix=config.full_origin_data_prefix
    )

    return [
        obj['Key'] for page in pages
        for obj in page.get('Contents', [])
        if obj['Key'].endswith('.jsonl')
    ]


def generate_s3_key(prefix: str) -> str:
    """
    Generates a unique, time-stamped S3 key for the output file.

    The key is structured with a date-based path to facilitate partitioning.
    Example: 'consolidated/2025/10/18/12/52/22_abcdef12/data.parquet'

    Returns:
        str: A unique S3 object key.
    """
    timestamp = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
    return f"{prefix}{timestamp}_data.parquet"


def upload_df_to_s3(s3_handler: S3Handler, df: pd.DataFrame, key: str):
    """
    Uploads a DataFrame to S3 as a PARQUET file.

    Args:
        s3_handler (S3Handler): An S3 handler configured for the destination bucket.
        df (pd.DataFrame): The pandas DataFrame to upload.
        key (str): The S3 object key for the destination file.
    """
    with io.BytesIO() as buffer:
        df.to_parquet(buffer, index=False, engine='pyarrow')
        s3_handler.s3_client.put_object(
            Bucket=config.bucket_destiny_name, Key=key, Body=buffer.getvalue()
        )
    logger.info(f"Successfully uploaded to s3://{config.bucket_destiny_name}/{key}")


def delete_files_from_s3(s3_handler: S3Handler, keys: List[str]):
    """
    Deletes a list of files from the origin S3 bucket.

    Args:
        s3_handler (S3Handler): An S3 handler configured for the origin bucket.
        keys (List[str]): A list of S3 object keys to be deleted.
    """
    if not keys:
        return
    objects_to_delete = [{'Key': key} for key in keys]
    s3_handler.s3_client.delete_objects(
        Bucket=config.bucket_origin_name, Delete={'Objects': objects_to_delete}
    )
    logger.info(f"Successfully deleted {len(keys)} source files.")

async def process_json_key_async(
    session: aioboto3.Session, key: str
) -> Optional[pd.DataFrame]:
    """
    Asynchronously downloads and processes a JSONL file line-by-line.

    It checks for a nested payload within each line, extracts it, and builds
    a DataFrame from the valid records.

    Args:
        session (aioboto3.Session): The aioboto3 session for creating a client.
        key (str): The S3 object key of the json to process.

    Returns:
        Optional[pd.DataFrame]: A DataFrame if valid data is found, otherwise None.
    """
    logger.debug(f"Processing {key}...")
    processed_records = []
    try:
        async with session.client("s3") as s3_client:
            response = await s3_client.get_object(
                Bucket=config.bucket_origin_name, Key=key
            )
            body = await response['Body'].read()
            
            for line in body.decode('utf-8').strip().splitlines():
                try:
                    record = json.loads(line)
                    payload_str = record.get('data')

                    if payload_str:
                        dict_str = payload_str.replace('payload=', '')
                        payload_dict = ast.literal_eval(dict_str)
                        processed_records.append(payload_dict)

                except (json.JSONDecodeError, SyntaxError, ValueError) as line_error:
                    logger.warning(f"Skipping malformed line in {key}: {line_error}")
                    continue

        if not processed_records:
            logger.warning(f"No valid records found in {key}.")
            return None

        df = pd.DataFrame(processed_records)
        df = df.astype(SCHEMA)

        for col in DATE_COLUMNS:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col])

        expected_columns = set(SCHEMA.keys()) | set(DATE_COLUMNS)
        if set(df.columns) != expected_columns:
            logger.warning(f"Final column mismatch in {key} after processing. Ignoring file.")
            return None
            
        return df
        
    except Exception as e:
        logger.error(f"A critical error occurred while processing {key}: {e}")
        return None


async def process_files_concurrently(
    aio_session: aioboto3.Session, json_list: List[str]
) -> List[pd.DataFrame]:
    """
    Orchestrates concurrent downloading and processing of json files.

    Args:
        aio_session (aioboto3.Session): The aioboto3 session to be used by tasks.
        json_list (List[str]): The list of S3 object keys to process.

    Returns:
        List[pd.DataFrame]: A list of valid DataFrames, excluding any that failed
            processing or validation.
    """
    tasks = [process_json_key_async(aio_session, key) for key in json_list]
    results = await asyncio.gather(*tasks)
    return [df for df in results if df is not None]


def main():
    """
    Main ETL process entry point.

    Initializes S3 handlers, finds and processes source json files concurrently,
    consolidates them into a single Parquet file, uploads it, and finally
    cleans up the source files.
    """
    logger.info("Initializing S3 handler for ORIGIN bucket...")
    origin_s3_handler = create_s3_handler(
        bucket_name=config.bucket_origin_name
    )

    logger.info("Initializing S3 handler for DESTINATION bucket...")
    dest_s3_handler = create_s3_handler(
        bucket_name=config.bucket_destiny_name
    )

    json_list = get_json_filenames(origin_s3_handler)

    if not json_list:
        logger.info("No json files found to process.")
        return

    logger.info(f"Found {len(json_list)} json files. Processing concurrently...")
    valid_dfs = asyncio.run(
        process_files_concurrently(origin_s3_handler.s3_aio_client, json_list)
    )

    if not valid_dfs:
        logger.info("No valid DataFrames were created. Exiting.")
        return

    try:
        logger.info(f"Merging {len(valid_dfs)} valid DataFrames...")
        final_df = pd.concat(valid_dfs, ignore_index=True)

        output_key = generate_s3_key(config.full_destiny_data_prefix)
        upload_df_to_s3(dest_s3_handler, final_df, output_key)

        logger.info("--- Cleanup ---")
        delete_files_from_s3(origin_s3_handler, json_list)
    except Exception as e:
        logger.error(f"A critical error occurred during merge or upload: {e}")
        logger.warning("Source files were NOT deleted due to the error.")
        raise