# -*- coding: utf-8 -*-
import json
import logging
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

def get_api_key(environment: str, region: str) -> str:
    """
    Get API key based on environment.
    
    Args:
        environment: The environment (development, staging, production)
        region: AWS region
        
    Returns:
        str: The API key
    """
    secret_name = f"wilows-api-secrets-{environment.lower()}"
    
    try:
        logger.info(f"Retrieving API key from secret: {secret_name}")
        secrets_manager = boto3.client('secretsmanager', region_name=region)
        response = secrets_manager.get_secret_value(SecretId=secret_name)
        secret_data = json.loads(response['SecretString'])
        
        api_key = secret_data.get('API_KEY')
        if not api_key:
            raise ValueError(f"API_KEY not found in secret {secret_name}")
        
        logger.info(f"Successfully retrieved API key from {secret_name}")
        return api_key
    
    except ClientError as e:
        logger.error(f"Failed to retrieve secret {secret_name}: {e}")
        raise ValueError(f"Cannot retrieve API key from AWS Secrets Manager: {e}")
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in secret {secret_name}: {e}")
        raise ValueError(f"Invalid secret format in {secret_name}: {e}")