# -*- coding: utf-8 -*-
import logging
from app.core.config import config
from app.services.s3_service import S3Handler

logger = logging.getLogger(__name__)

# Global S3 service instance - will be initialized at startup
_s3_service: S3Handler = None

def initialize_s3_service() -> None:
    """
    Initialize S3 service at application startup.
    Called from the lifespan context manager in main.py
    
    Creates a global S3Handler instance using configuration values and tests
    the connection by performing a head_bucket operation.
    
    Raises:
        Exception: If the S3 service initialization or connection test fails.
    """
    global _s3_service
    try:    
        aws_access_key = config.aws_access_key_id
        aws_secret_key = config.aws_secret_access_key
        
        logger.info(f"Creating S3Handler for Region: {config.region}, Bucket: {config.bucket_name}")
        
        _s3_service = S3Handler(
            bucket_name=config.bucket_name,
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region=config.region
        )
        
        # Test the connection - this will raise an exception if it fails
        logger.info("Testing S3 connection...")
        _s3_service.s3_client.head_bucket(Bucket=config.bucket_name)
        logger.info(f"S3 service initialized and tested successfully for bucket: {config.bucket_name}")
        
    except Exception as e:
        logger.error(f"Failed to initialize S3 service: {e}")
        raise Exception(f"Failed to connect to S3: {e}")

def get_s3_service() -> S3Handler:
    """
    Simple dependency function for FastAPI routes.
    
    Returns: 
        S3Handler: The initialized S3 service instance for handling S3 operations.
    """
    return _s3_service