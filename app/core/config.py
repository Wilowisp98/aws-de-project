# -*- coding: utf-8 -*-
from typing import Optional
from pydantic_settings import BaseSettings
from dotenv import load_dotenv
from .secrets import get_api_key

load_dotenv()

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
    bucket_name: str  # Required
    
    # Data pipeline settings (required)
    data_prefix: str
    
    # Security settings (will be loaded based on environment)
    api_key: Optional[str] = None
    
    # Optional settings with defaults
    debug: Optional[bool] = False
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # For non-development environments, load API key from AWS Secrets Manager
        if not self.api_key:
            self.api_key = get_api_key(self.environment, self.region)
    
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
    def full_data_prefix(self) -> str:
        """
        Combines the environment-specific prefix with the configured data prefix
        to create the complete S3 key prefix for data storage.
        
        Returns:
            str: Complete S3 key prefix combining environment and data prefixes.
                Examples: "dev/data/", "staging/data/", "data/" (for production)
        """
        return f"{self.environment_prefix}{self.data_prefix}"

# Global settings instance
config = Config()