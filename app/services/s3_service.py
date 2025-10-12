# -*- coding: utf-8 -*-
import boto3
from botocore.config import Config

class S3Handler:
    def __init__(self, bucket_name, aws_access_key_id=None, aws_secret_access_key=None, region='eu-west-1'):
        """
        Initialize S3 connection with flexible authentication
        
        Args:
            bucket_name (str): Name of your S3 bucket
            aws_access_key_id (str, optional): AWS Access Key ID (for local dev)
            aws_secret_access_key (str, optional): AWS Secret Access Key (for local dev)
            region (str): AWS region
        
        Note:
            - If keys are provided: Uses explicit credentials (local development)
            - If keys are None: Uses IAM role/default credentials (AWS Lambda/EC2)
        """
        self.bucket_name = bucket_name
        self.region = region
        
        # Configure boto3 with timeouts to prevent hanging
        boto_config = Config(
            connect_timeout=10,
            read_timeout=10,
            retries={'max_attempts': 2}
        )
        
        # Create S3 client with flexible authentication
        if aws_access_key_id and aws_secret_access_key:
            # Explicit credentials (local development)
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                region_name=region,
                config=boto_config
            )
        else:
            # Use default AWS credentials chain (IAM roles, environment variables, etc.)
            self.s3_client = boto3.client(
                's3', 
                region_name=region, 
                config=boto_config
            )