# -*- coding: utf-8 -*-
from datetime import datetime, timezone
from utils.s3_service import S3Handler
from app.core.config import config

def generate_timestamped_filename() -> str:
    """
    Generate a timestamp-based filename for incoming data.
    
    Returns:
        str: Filename with timestamp (e.g., 'raw_data_20251011_143052_123456.jsonl')
    """
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
    return f"raw_data_{timestamp}.jsonl"

def get_s3_key(filename: str) -> str:
    """
    Get the environment-aware S3 key for a timestamped file.
    
    Args:
        filename: The timestamped filename
        
    Returns:
        str: S3 key with environment prefix (e.g., dev/data/incoming/filename or data/incoming/filename)
    """
    return f"{config.full_data_prefix}{filename}"

def get_full_s3_path(s3_service: S3Handler, filename: str) -> str:
    """
    Get the full S3 path for a timestamped file.
    
    Args:
        s3_service: S3Handler instance
        filename: The timestamped filename
        
    Returns:
        str: Full S3 path (s3://bucket/prefix/filename)
    """
    s3_key = get_s3_key(filename)
    return f"s3://{s3_service.bucket_name}/{s3_key}"