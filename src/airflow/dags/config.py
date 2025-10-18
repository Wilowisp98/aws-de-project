# -*- coding: utf-8 -*-
from typing import Optional
from pydantic_settings import BaseSettings
from pathlib import Path
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
dotenv_path = PROJECT_ROOT / "env/.env.airflow"
load_dotenv(dotenv_path=dotenv_path)

class Config(BaseSettings):
    """
    Application settings loaded from environment variables.
    """
    
    # Environment settings (required)
    environment: str  # development, staging, production
    log_level: str
    app_name: str
    
    # AWS settings
    aws_access_key_id: Optional[str] = None  # Optional: for local dev. Use IAM roles on EC2/Lambda
    aws_secret_access_key: Optional[str] = None  # Optional: for local dev. Use IAM roles on EC2/Lambda
    region: str  # Required
    bucket_origin_name: str  # Required
    bucket_destiny_name: str # Required
    
    # Data pipeline settings (required)
    origin_data_prefix: str
    destiny_data_prefix: str
    
    # Optional settings with defaults
    debug: Optional[bool] = False
    
    @property
    def environment_prefix(self) -> str:
        """
        Maps the current environment to its corresponding S3 key prefix.
        Development and staging use prefixed paths, while production uses root level.
        
        Returns:
            str: Environment-specific prefix:
                - "dev/" for development environment
                - "staging/" for staging environment  
                - "" (empty string) for production environment
        """
        env_prefixes = {
            "development": "dev/",
            "staging": "staging/", 
            "production": ""
        }
        env_key = self.environment.lower()
        return env_prefixes[env_key]
    
    @property
    def full_origin_data_prefix(self) -> str:
        """
        Combines the environment-specific prefix with the configured data prefix
        to create the complete S3 key prefix for data storage.
        
        Returns:
            str: Complete S3 key prefix combining environment and data prefixes.
                Examples: "dev/data/", "staging/data/", "data/" (for production)
        """
        return f"{self.environment_prefix}{self.origin_data_prefix}"
    
    @property
    def full_destiny_data_prefix(self) -> str:
        """
        Combines the environment-specific prefix with the configured data prefix
        to create the complete S3 key prefix for data storage.
        
        Returns:
            str: Complete S3 key prefix combining environment and data prefixes.
                Examples: "dev/data/", "staging/data/", "data/" (for production)
        """
        return f"{self.environment_prefix}{self.destiny_data_prefix}"
    
# Global settings instance
config = Config()