# -*- coding: utf-8 -*-
from fastapi import APIRouter, Depends, HTTPException
from typing import Dict, Any
import json
from datetime import datetime, timezone
from utils.dependencies import get_api_s3_handler_ingestion_bucket
from utils.s3_service import S3Handler
from app.api.utils import generate_timestamped_filename, get_s3_key
from app.core.security import require_api_key
import logging
from pydantic import BaseModel, Field
from typing import Dict, Any

logger = logging.getLogger(__name__)

router = APIRouter()

class IngestRequest(BaseModel):
    payload: Dict[str, Any] = Field(..., description="The actual data to ingest")

@router.post("/ingest")
def ingest_data(
    payload: IngestRequest,
    s3_service: S3Handler = Depends(get_api_s3_handler_ingestion_bucket),
    _: str = Depends(require_api_key)
):
    """
    Takes the provided payload, enriches it with a UTC timestamp and uploads
    it as a JSON file to the configured S3 bucket using a timestamped filename.
    
    Args:
        payload: The data to ingest, wrapped in an IngestRequest model.
        s3_service: S3Handler instance for S3 operations (injected dependency).
        _: API key dependency (validated by require_api_key).
    
    Returns:
        dict: A dictionary containing a success message:
            - message: "Data ingested successfully"
    
    Raises:
        HTTPException: 
            - 400: If the data contains invalid JSON format
            - 502: If S3 upload fails
            - 500: For any other unexpected errors during ingestion
    """
    try:
        enriched_data = {
            "timestamp": datetime.now(timezone.utc),
            "data": payload
        }
        json_content = json.dumps(enriched_data, default=str)
        filename = generate_timestamped_filename()
        s3_key = get_s3_key(filename)
        s3_service.s3_client.put_object(
            Bucket=s3_service.bucket_name,
            Key=s3_key,
            Body=json_content.encode('utf-8'),
            ContentType='application/json'
        )
        
        return {
            "message": "Data ingested successfully"
        }
    except Exception as e:
        logger.error(f"Unexpected error in data ingestion: {str(e)}")
        return {
            "status": "error", "message": str(e)
        }