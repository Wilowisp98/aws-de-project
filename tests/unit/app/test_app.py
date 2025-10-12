# -*- coding: utf-8 -*-
"""
Unit tests for the FastAPI S3 Ingestion API.
This file contains tests for the main components of the application.
"""
import pytest
from unittest.mock import Mock, patch
from fastapi.testclient import TestClient
import os
import sys

# Add the project root to Python path for imports
project_root = os.path.join(os.path.dirname(__file__), "..", "..", "..")
sys.path.insert(0, project_root)

from app.main import create_app
from app.services.s3_service import S3Handler
from app.api.utils import generate_timestamped_filename, get_s3_key, get_full_s3_path
from app.core.config import Config


class TestConfig:
    """Tests for configuration management."""
    
    def test_config_environment_prefix_development(self):
        """Test that development environment returns correct prefix."""
        config = Config(
            environment="development",
            log_level="INFO",
            app_name="Test App",
            region="us-east-1",
            bucket_name="test-bucket",
            data_prefix="data/",
            api_key="test-key"
        )
        assert config.environment_prefix == "dev/"
    
    def test_config_environment_prefix_production(self):
        """Test that production environment returns empty prefix."""
        config = Config(
            environment="production",
            log_level="INFO",
            app_name="Test App",
            region="us-east-1",
            bucket_name="test-bucket",
            data_prefix="data/",
            api_key="test-key"
        )
        assert config.environment_prefix == ""
    
    def test_config_full_data_prefix(self):
        """Test that full data prefix combines environment and data prefixes."""
        config = Config(
            environment="staging",
            log_level="INFO",
            app_name="Test App",
            region="us-east-1",
            bucket_name="test-bucket",
            data_prefix="data/",
            api_key="test-key"
        )
        assert config.full_data_prefix == "staging/data/"


class TestS3Handler:
    """Tests for S3Handler service."""
    
    @patch('boto3.client')
    def test_s3_handler_init_with_credentials(self, mock_boto_client):
        """Test S3Handler initialization with explicit credentials."""
        mock_client = Mock()
        mock_boto_client.return_value = mock_client
        
        handler = S3Handler(
            bucket_name="test-bucket",
            aws_access_key_id="test-key",
            aws_secret_access_key="test-secret",
            region="us-east-1"
        )
        
        assert handler.bucket_name == "test-bucket"
        assert handler.region == "us-east-1"
        mock_boto_client.assert_called_once()
    
    @patch('boto3.client')
    def test_s3_handler_init_without_credentials(self, mock_boto_client):
        """Test S3Handler initialization using default AWS credentials."""
        mock_client = Mock()
        mock_boto_client.return_value = mock_client
        
        handler = S3Handler(
            bucket_name="test-bucket",
            region="us-west-2"
        )
        
        assert handler.bucket_name == "test-bucket"
        assert handler.region == "us-west-2"
        mock_boto_client.assert_called_once()


class TestUtils:
    """Tests for utility functions."""
    
    def test_generate_timestamped_filename(self):
        """Test that timestamped filename has correct format."""
        filename = generate_timestamped_filename()
        
        # Check that it starts with 'raw_data_' and ends with '.jsonl'
        assert filename.startswith("raw_data_")
        assert filename.endswith(".jsonl")
        
        # Check that it contains underscores (timestamp format)
        assert filename.count("_") >= 3  # raw_data_YYYYMMDD_HHMMSS_microseconds
    
    @patch('app.api.utils.config')
    def test_get_s3_key(self, mock_config):
        """Test S3 key generation with environment prefix."""
        mock_config.full_data_prefix = "dev/data/"
        
        filename = "test_file.jsonl"
        s3_key = get_s3_key(filename)
        
        assert s3_key == "dev/data/test_file.jsonl"
    
    @patch('app.api.utils.config')
    def test_get_full_s3_path(self, mock_config):
        """Test full S3 path generation."""
        mock_config.full_data_prefix = "staging/data/"
        
        # Create a mock S3Handler
        s3_service = Mock()
        s3_service.bucket_name = "my-test-bucket"
        
        filename = "test_file.jsonl"
        full_path = get_full_s3_path(s3_service, filename)
        
        assert full_path == "s3://my-test-bucket/staging/data/test_file.jsonl"


class TestAPIEndpoints:
    """Tests for API endpoints."""
    
    @pytest.fixture
    def mock_config(self):
        """Create a mock configuration for testing."""
        with patch('app.core.config.config') as mock_config:
            mock_config.app_name = "Test API"
            mock_config.environment = "development"
            mock_config.environment_prefix = "dev/"
            mock_config.api_key = "test-api-key"
            mock_config.bucket_name = "test-bucket"
            mock_config.region = "us-east-1"
            mock_config.full_data_prefix = "dev/data/"
            yield mock_config
    
    @pytest.fixture
    def client(self, mock_config):
        """Create a test client with mocked dependencies."""
        with patch('app.api.dependencies.initialize_s3_service'):
            with patch('app.api.dependencies._s3_service') as mock_s3:
                with patch('app.core.security.config', mock_config):
                    mock_s3.s3_client = Mock()
                    mock_s3.bucket_name = "test-bucket"
                    mock_s3.region = "us-east-1"
                    
                    app = create_app()
                    return TestClient(app)
    
    def test_health_endpoint(self, client):
        """Test the health check endpoint."""
        response = client.get("/api/dev/v1/health")
        
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert data["message"] == "API is running"
    
    def test_s3_status_endpoint_success(self, client):
        """Test S3 status endpoint with successful connection."""
        with patch('app.api.routes.health._s3_service') as mock_s3:
            with patch('app.core.security.config.api_key', "test-api-key"):
                mock_s3.s3_client.head_bucket.return_value = None  # Successful call
                mock_s3.bucket_name = "test-bucket"
                mock_s3.region = "us-east-1"
                
                response = client.get(
                    "/api/dev/v1/s3-status",
                    headers={"Authorization": "Bearer test-api-key"}
                )
            
            assert response.status_code == 200
            data = response.json()
            assert data["status"] == "connected"
            assert data["bucket"] == "test-bucket"
            assert data["region"] == "us-east-1"
    
    def test_s3_status_endpoint_unauthorized(self, client):
        """Test S3 status endpoint without API key."""
        response = client.get("/api/dev/v1/s3-status")
        
        assert response.status_code == 403  # FastAPI returns 403 when no Authorization header
    
    @patch('app.api.routes.data.generate_timestamped_filename')
    @patch('app.api.routes.data.get_s3_key')
    def test_ingest_endpoint_success(self, mock_get_s3_key, mock_generate_filename, client):
        """Test successful data ingestion."""
        # Setup mocks
        mock_generate_filename.return_value = "test_file.jsonl"
        mock_get_s3_key.return_value = "dev/data/test_file.jsonl"
        
        with patch('app.api.dependencies._s3_service') as mock_s3:
            with patch('app.core.security.config.api_key', "test-api-key"):
                mock_s3.s3_client.put_object.return_value = None
                mock_s3.bucket_name = "test-bucket"
                
                # Test data
                test_payload = {
                    "payload": {
                        "user_id": 123,
                        "action": "click",
                        "timestamp": "2025-10-12T10:00:00Z"
                    }
                }
                
                response = client.post(
                    "/api/dev/v1/data/ingest",
                    json=test_payload,
                    headers={"Authorization": "Bearer test-api-key"}
                )
            
            assert response.status_code == 200
            data = response.json()
            assert data["message"] == "Data ingested successfully"
    
    def test_ingest_endpoint_unauthorized(self, client):
        """Test data ingestion without API key."""
        test_payload = {
            "payload": {
                "user_id": 123,
                "action": "click"
            }
        }
        
        response = client.post("/api/dev/v1/data/ingest", json=test_payload)
        
        assert response.status_code == 403  # FastAPI returns 403 when no Authorization header
    
    def test_ingest_endpoint_invalid_payload(self, client):
        """Test data ingestion with invalid payload."""
        with patch('app.core.security.config.api_key', "test-api-key"):
            # Send invalid JSON (missing required 'payload' field)
            response = client.post(
                "/api/dev/v1/data/ingest",
                json={"wrong_field": "data"},
                headers={"Authorization": "Bearer test-api-key"}
            )
        
        assert response.status_code == 422  # Validation error


if __name__ == "__main__":
    pytest.main([__file__, "-v"])