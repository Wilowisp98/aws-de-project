# -*- coding: utf-8 -*-
import logging
from fastapi import HTTPException, Security, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from app.core.config import config

logger = logging.getLogger(__name__)

# Security scheme
security = HTTPBearer()

def verify_api_key(credentials: HTTPAuthorizationCredentials = Security(security)) -> str:
    """
    Verify API key authentication.
    
    Args:
        credentials: HTTP Bearer token credentials
        
    Returns:
        str: The validated API key
        
    Raises:
        HTTPException: 401 if API key is invalid or missing
    """    
    if not credentials or credentials.credentials != config.api_key:
        logger.warning(f"Invalid API key attempt: {credentials.credentials[:8] if credentials else 'None'}...")
        raise HTTPException(
            status_code=401,
            detail="Invalid or missing API key",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    logger.info("API key authentication successful")
    return credentials.credentials

# Dependency for protected routes
def require_api_key(api_key: str = Depends(verify_api_key)) -> str:
    """
    This is a convenience wrapper around verify_api_key that can be used
    directly as a FastAPI dependency to protect routes that require authentication.
    
    Args:
        api_key: The validated API key from verify_api_key dependency.
    
    Returns:
        str: The validated API key passed through from verify_api_key.
    
    Usage:
        @router.post("/protected-endpoint")
        def protected_endpoint(api_key: str = Depends(require_api_key)):
            # This endpoint requires valid API key
            pass
    """
    return api_key