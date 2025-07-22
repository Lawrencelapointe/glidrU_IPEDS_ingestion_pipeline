# Copyright (c) 2025 Blue Sky Mind LLC
# All Rights Reserved.
# Proprietary and confidential.

"""
ConfigManager: Centralized configuration and environment variable management.

This module handles parsing of config.ini and .env files, ensuring that all
configuration values are accessed through a single, testable interface.
"""

import os
from configparser import ConfigParser
from pathlib import Path
from typing import Any, Optional

from dotenv import load_dotenv
from pydantic import BaseModel, Field, ValidationError


class PathConfig(BaseModel):
    """Configuration for file paths and storage locations."""
    
    raw_bucket: str = Field(..., description="GCS bucket for raw IPEDS data")
    staging_dataset: str = Field(..., description="BigQuery dataset for staging tables")
    mart_dataset: str = Field(..., description="BigQuery dataset for mart tables")
    temp_dir: str = Field(default="/tmp/windsurf-ipeds", description="Temporary directory for downloads")


class IPEDSConfig(BaseModel):
    """Configuration specific to IPEDS data sources."""
    
    default_year: int = Field(2024, description="Default year for IPEDS data")
    mdb_base_url: str = Field(..., description="Base URL for IPEDS MDB downloads")
    provisional_suffix: str = Field("_P", description="Suffix for provisional data")
    final_suffix: str = Field("_F", description="Suffix for final data")


class BigQueryConfig(BaseModel):
    """BigQuery-specific configuration."""
    
    location: str = Field("US", description="BigQuery dataset location")
    write_disposition: str = Field("WRITE_TRUNCATE", description="BigQuery write disposition")
    create_disposition: str = Field("CREATE_IF_NEEDED", description="BigQuery create disposition")


class LoggingConfig(BaseModel):
    """Logging configuration."""
    
    level: str = Field("INFO", description="Logging level")
    format: str = Field(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        description="Log message format"
    )


class Config(BaseModel):
    """Complete configuration container."""
    
    paths: PathConfig
    ipeds: IPEDSConfig
    bigquery: BigQueryConfig
    logging: LoggingConfig


class ConfigManager:
    """
    Manages configuration loading from config.ini and environment variables.
    
    This class ensures that:
    - All configuration is loaded from predictable locations
    - Environment variables override config file values
    - Missing required values raise clear errors
    - Configuration is validated using Pydantic models
    """
    
    def __init__(self, config_path: Optional[Path] = None, env_path: Optional[Path] = None):
        """
        Initialize the ConfigManager.
        
        Args:
            config_path: Path to config.ini file. Defaults to config/config.ini
            env_path: Path to .env file. Defaults to .env in project root
        """
        self._config_path = config_path or self._find_config_file()
        self._env_path = env_path or self._find_env_file()
        
        # Load environment variables first (they take precedence)
        if self._env_path.exists():
            load_dotenv(self._env_path)
        
        # Parse config.ini
        self._parser = ConfigParser()
        if not self._config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {self._config_path}")
        
        self._parser.read(self._config_path)
        
        # Load and validate configuration
        self._config = self._load_config()
    
    @property
    def config(self) -> Config:
        """Get the validated configuration object."""
        return self._config
    
    def get_env(self, key: str, default: Optional[str] = None, required: bool = False) -> Optional[str]:
        """
        Get an environment variable value.
        
        Args:
            key: Environment variable name
            default: Default value if not found
            required: If True, raise error if not found and no default
            
        Returns:
            Environment variable value or default
            
        Raises:
            ValueError: If required=True and variable not found
        """
        value = os.getenv(key, default)
        if required and value is None:
            raise ValueError(f"Required environment variable not found: {key}")
        return value
    
    def get_secret(self, key: str) -> str:
        """
        Get a required secret from environment variables.
        
        Args:
            key: Environment variable name
            
        Returns:
            Secret value
            
        Raises:
            ValueError: If secret not found
        """
        return self.get_env(key, required=True)  # type: ignore
    
    def _find_config_file(self) -> Path:
        """Find the config.ini file by searching up from current directory."""
        current = Path.cwd()
        while current != current.parent:
            config_file = current / "config" / "config.ini"
            if config_file.exists():
                return config_file
            current = current.parent
        
        # Default location
        return Path("config/config.ini")
    
    def _find_env_file(self) -> Path:
        """Find the .env file in project root."""
        current = Path.cwd()
        while current != current.parent:
            env_file = current / ".env"
            if env_file.exists():
                return env_file
            # Also check for pyproject.toml to identify project root
            if (current / "pyproject.toml").exists():
                return current / ".env"
            current = current.parent
        
        # Default to current directory
        return Path(".env")
    
    def _load_config(self) -> Config:
        """Load and validate configuration from config.ini."""
        try:
            config_dict = {
                section: dict(self._parser.items(section))
                for section in self._parser.sections()
            }
            
            # Convert specific fields to correct types
            if "ipeds" in config_dict:
                config_dict["ipeds"]["default_year"] = int(
                    config_dict["ipeds"].get("default_year", 2024)
                )
            
            return Config(**config_dict)
            
        except ValidationError as e:
            raise ValueError(f"Invalid configuration: {e}")
    
    def get_credentials_path(self) -> Path:
        """Get the path to Google Cloud credentials."""
        creds = self.get_env("GOOGLE_APPLICATION_CREDENTIALS")
        if not creds:
            raise ValueError(
                "GOOGLE_APPLICATION_CREDENTIALS environment variable not set. "
                "Please set it to the path of your service account JSON file."
            )
        
        path = Path(creds)
        if not path.exists():
            raise FileNotFoundError(f"Credentials file not found: {path}")
        
        return path
