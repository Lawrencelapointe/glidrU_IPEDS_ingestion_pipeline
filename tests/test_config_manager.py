# Copyright (c) 2025 Blue Sky Mind LLC
# All Rights Reserved.
# Proprietary and confidential.

"""
Unit tests for ConfigManager.

Tests configuration loading, environment variable handling, and validation.
"""

import os
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest
from src.core.config_manager import ConfigManager, Config


@pytest.fixture
def temp_config_file():
    """Create a temporary config.ini file for testing."""
    content = """[paths]
raw_bucket = gs://test-ipeds-raw
staging_dataset = test_staging
mart_dataset = test_mart
temp_dir = /tmp/test-glidru

[ipeds]
default_year = 2023
mdb_base_url = https://test.example.com/ipeds
provisional_suffix = _TEST_P
final_suffix = _TEST_F

[bigquery]
location = EU
write_disposition = WRITE_APPEND
create_disposition = CREATE_NEVER

[logging]
level = DEBUG
format = test-format
"""
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.ini', delete=False) as f:
        f.write(content)
        temp_path = Path(f.name)
    
    yield temp_path
    
    # Cleanup
    temp_path.unlink()


@pytest.fixture
def temp_env_file():
    """Create a temporary .env file for testing."""
    content = """GOOGLE_APPLICATION_CREDENTIALS=/tmp/test-creds.json
SLACK_WEBHOOK_URL=https://hooks.slack.com/test
TEST_SECRET=secret_value
"""
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.env', delete=False) as f:
        f.write(content)
        temp_path = Path(f.name)
    
    yield temp_path
    
    # Cleanup
    temp_path.unlink()


class TestConfigManager:
    """Test cases for ConfigManager."""
    
    def test_load_config_from_file(self, temp_config_file):
        """Test loading configuration from config.ini."""
        manager = ConfigManager(config_path=temp_config_file)
        config = manager.config
        
        # Test paths section
        assert config.paths.raw_bucket == "gs://test-ipeds-raw"
        assert config.paths.staging_dataset == "test_staging"
        assert config.paths.mart_dataset == "test_mart"
        assert config.paths.temp_dir == "/tmp/test-glidru"
        
        # Test ipeds section
        assert config.ipeds.default_year == 2023
        assert config.ipeds.mdb_base_url == "https://test.example.com/ipeds"
        assert config.ipeds.provisional_suffix == "_TEST_P"
        assert config.ipeds.final_suffix == "_TEST_F"
        
        # Test bigquery section
        assert config.bigquery.location == "EU"
        assert config.bigquery.write_disposition == "WRITE_APPEND"
        assert config.bigquery.create_disposition == "CREATE_NEVER"
        
        # Test logging section
        assert config.logging.level == "DEBUG"
        assert config.logging.format == "test-format"
    
    def test_missing_config_file(self):
        """Test error when config file doesn't exist."""
        with pytest.raises(FileNotFoundError, match="Configuration file not found"):
            ConfigManager(config_path=Path("/nonexistent/config.ini"))
    
    def test_env_variable_loading(self, temp_config_file, temp_env_file):
        """Test loading environment variables from .env file."""
        manager = ConfigManager(config_path=temp_config_file, env_path=temp_env_file)
        
        # Check that env vars are loaded
        assert manager.get_env("SLACK_WEBHOOK_URL") == "https://hooks.slack.com/test"
        assert manager.get_env("TEST_SECRET") == "secret_value"
    
    def test_get_env_with_default(self, temp_config_file):
        """Test getting environment variable with default value."""
        manager = ConfigManager(config_path=temp_config_file)
        
        # Non-existent var should return default
        assert manager.get_env("NONEXISTENT", "default") == "default"
        
        # Existing var should return its value
        with patch.dict(os.environ, {"EXISTING": "value"}):
            assert manager.get_env("EXISTING", "default") == "value"
    
    def test_get_env_required(self, temp_config_file):
        """Test required environment variable."""
        manager = ConfigManager(config_path=temp_config_file)
        
        # Should raise error for missing required var
        with pytest.raises(ValueError, match="Required environment variable not found: MISSING"):
            manager.get_env("MISSING", required=True)
        
        # Should return value for existing required var
        with patch.dict(os.environ, {"EXISTING": "value"}):
            assert manager.get_env("EXISTING", required=True) == "value"
    
    def test_get_secret(self, temp_config_file):
        """Test getting secrets from environment."""
        manager = ConfigManager(config_path=temp_config_file)
        
        # Should raise error for missing secret
        with pytest.raises(ValueError, match="Required environment variable not found"):
            manager.get_secret("MISSING_SECRET")
        
        # Should return value for existing secret
        with patch.dict(os.environ, {"API_KEY": "secret123"}):
            assert manager.get_secret("API_KEY") == "secret123"
    
    def test_get_credentials_path(self, temp_config_file):
        """Test getting Google Cloud credentials path."""
        manager = ConfigManager(config_path=temp_config_file)
        
        # Should raise error when not set
        # Temporarily clear the environment variable for this test
        original_value = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
        if "GOOGLE_APPLICATION_CREDENTIALS" in os.environ:
            del os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
        
        try:
            with pytest.raises(ValueError, match="GOOGLE_APPLICATION_CREDENTIALS"):
                manager.get_credentials_path()
        finally:
            # Restore original value if it existed
            if original_value is not None:
                os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = original_value
        
        # Should return path when set and file exists
        with tempfile.NamedTemporaryFile() as f:
            with patch.dict(os.environ, {"GOOGLE_APPLICATION_CREDENTIALS": f.name}):
                path = manager.get_credentials_path()
                assert path == Path(f.name)
        
        # Should raise error when file doesn't exist
        with patch.dict(os.environ, {"GOOGLE_APPLICATION_CREDENTIALS": "/nonexistent/creds.json"}):
            with pytest.raises(FileNotFoundError, match="Credentials file not found"):
                manager.get_credentials_path()
    
    def test_config_validation(self, temp_config_file):
        """Test that configuration is validated properly."""
        # Create invalid config
        invalid_content = """[paths]
# Missing required fields
raw_bucket = gs://test-bucket

[ipeds]
default_year = not_a_number
"""
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.ini', delete=False) as f:
            f.write(invalid_content)
            invalid_path = Path(f.name)
        
        try:
            with pytest.raises(ValueError, match="Invalid configuration"):
                ConfigManager(config_path=invalid_path)
        finally:
            invalid_path.unlink()
    
    def test_config_type_conversion(self, temp_config_file):
        """Test that configuration values are converted to correct types."""
        manager = ConfigManager(config_path=temp_config_file)
        config = manager.config
        
        # Year should be converted to int
        assert isinstance(config.ipeds.default_year, int)
        assert config.ipeds.default_year == 2023


class TestConfigModels:
    """Test Pydantic configuration models."""
    
    def test_config_model_validation(self):
        """Test Config model validation."""
        valid_data = {
            "paths": {
                "raw_bucket": "gs://bucket",
                "staging_dataset": "staging",
                "mart_dataset": "mart"
            },
            "ipeds": {
                "mdb_base_url": "https://example.com"
            },
            "bigquery": {},
            "logging": {}
        }
        
        # Should create config with defaults
        config = Config(**valid_data)
        assert config.ipeds.default_year == 2024
        assert config.bigquery.location == "US"
        assert config.logging.level == "INFO"
    
    def test_missing_required_fields(self):
        """Test that missing required fields raise errors."""
        invalid_data = {
            "paths": {
                # Missing required fields
                "staging_dataset": "staging"
            },
            "ipeds": {},
            "bigquery": {},
            "logging": {}
        }
        
        with pytest.raises(ValueError):
            Config(**invalid_data)
