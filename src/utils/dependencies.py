# -*- coding: utf-8 -*-
import logging
from airflow.dags.config import config
from utils.s3_service import S3Handler
from functools import lru_cache

logger = logging.getLogger(__name__)

def create_s3_handler(bucket_name) -> S3Handler:
    """
    Creates and returns a new S3Handler instance for a specific bucket.

    This factory function initializes an S3Handler and tests the connection
    by performing a head_bucket operation.

    Args:
        bucket_name (str): The name of the S3 bucket to connect to.

    Returns:
        S3Handler: A connected and tested S3 handler instance.
    
    Raises:
        Exception: If the S3 service initialization or connection test fails.
    """
    try:
        aws_access_key = config.aws_access_key_id
        aws_secret_key = config.aws_secret_access_key
        
        logger.info(f"Creating S3Handler for Region: {config.region}, Bucket: {bucket_name}")
        
        s3_service = S3Handler(
            bucket_name=bucket_name,
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region=config.region
        )
        
        # Test the connection
        logger.info(f"Testing S3 connection for bucket: {bucket_name}...")
        s3_service.s3_client.head_bucket(Bucket=bucket_name)
        logger.info(f"S3 service tested successfully for bucket: {bucket_name}")
        
        return s3_service
        
    except Exception as e:
        logger.error(f"Failed to initialize S3 service for bucket {bucket_name}: {e}")
        raise Exception(f"Failed to connect to S3 bucket {bucket_name}: {e}")
    
@lru_cache()
def get_api_s3_handler_ingestion_bucket() -> S3Handler:
    """
    FastAPI dependency provider for the API's S3 handler.
    Caches the handler instance for efficiency.
    """
    from app.core.config import config
    return create_s3_handler(config.bucket_name)