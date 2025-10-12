# -*- coding: utf-8 -*-
from fastapi import FastAPI
from contextlib import asynccontextmanager

from app.core.config import config
from app.core.logging import setup_logging
logger = setup_logging()

from app.api.routes import health, data

@asynccontextmanager
async def lifespan(_: FastAPI):
    """
    Handles the complete application lifecycle:
    - Startup: Initializes logging, validates configuration and establishes S3 connection
    - Shutdown: Performs cleanup operations
    
    The application will fail to start if S3 connection cannot be established,
    ensuring that the API only runs when all required services are available.
    
    Args:
        app: FastAPI application instance.
        
    Yields:
        None: Control to the running application.
        
    Raises:
        Exception: If S3 service initialization fails during startup.
    """
    logger.info("Starting application...")
    logger.info(f"Debug mode: {config.debug}")
    logger.info(f"Log level: {config.log_level}")
    
    # Initialize S3 connection at startup - FAIL if S3 unavailable
    try:
        from app.api.dependencies import initialize_s3_service
        logger.info("Initializing S3 connection...")
        logger.info(f"Configuration loaded - Region: {config.region}, Bucket: {config.bucket_name}")
        initialize_s3_service()
        logger.info("S3 connection established successfully!")
    except Exception as e:
        logger.error(f"Failed to initialize S3 service: {e}")
        logger.error("Application startup failed - S3 connection is required")
        raise e
    
    yield
    
    # Shutdown
    logger.info("Shutting down application...")

def create_app() -> FastAPI:
    """
    Initializes a FastAPI instance with environment-specific configuration,
    sets up dynamic API prefixes based on the deployment environment
    and registers all application routers with appropriate URL prefixes.
    
    API URL Structure:
    - Production: /api/v1/* 
    - Development: /api/dev/v1/*
    - Staging: /api/staging/v1/*
    
    Returns:
        FastAPI: Configured FastAPI application instance with all routes registered.
    """
    app = FastAPI(
        title=config.app_name,
        description="API for S3 Ingestion",
        version="1.0.0",
        lifespan=lifespan
    )
    
    # Dynamic API prefix based on environment
    # Production: /api/v1
    # Development: /api/dev/v1  
    # Staging: /api/staging/v1
    if config.environment.lower() == "production":
        api_prefix = "/api/v1"
        data_prefix = "/api/v1/data"
    else:
        env = config.environment_prefix.rstrip('/')  # Remove trailing slash for URL
        api_prefix = f"/api/{env}/v1"
        data_prefix = f"/api/{env}/v1/data"
    
    # Include routers with dynamic prefixes
    # To add new routes, simply replicate what's being done below and create a new route on the app/api/routes folder.
    app.include_router(health.router, prefix=api_prefix, tags=["health"])
    app.include_router(data.router, prefix=data_prefix, tags=["data"])
    
    return app

# Create app instance
app = create_app()