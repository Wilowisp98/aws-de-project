# -*- coding: utf-8 -*-
from fastapi import APIRouter, Depends
from app.api.dependencies import _s3_service
from app.core.security import require_api_key

router = APIRouter()

@router.get("/health")
def health_check():
    """
    Simple endpoint to verify that the API is running and responsive.
    
    Returns:
        dict: A dictionary containing:
            - status: Always "healthy" 
            - message: Always "API is running"
    """
    return {"status": "healthy", "message": "API is running"}

@router.get("/s3-status")
def s3_status(_: str = Depends(require_api_key)):
    """
    Tests the S3 connection by performing a head_bucket operation and returns
    the connection status along with bucket and region information.
    
    Args:
        _: API key dependency (validated by require_api_key).
    
    Returns:
        dict: A dictionary containing:
            - status: "connected" if successful, "error" if failed
            - bucket: S3 bucket name (if connected)
            - region: S3 region (if connected)
            - message: Error message (if failed)
    """
    try:
        s3_service = _s3_service
        # Test S3 connection
        s3_service.s3_client.head_bucket(Bucket=s3_service.bucket_name)
        return {
            "status": "connected", 
            "bucket": s3_service.bucket_name,
            "region": s3_service.region
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}