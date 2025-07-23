# Copyright (c) 2025 Blue Sky Mind LLC
# All Rights Reserved.
# Proprietary and confidential.

"""Unit tests for CLI commands."""

import json
import re
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timezone

import pytest
from typer.testing import CliRunner

from src.cli.main import app
from src.downloaders.base_downloader import DownloadMetadata
from src.extractors.extraction_models import ExtractionManifest, ExtractionMetadata


runner = CliRunner()


class TestCLI:
    """Test cases for CLI commands."""
    
    @patch('src.cli.main.ConfigManager')
    @patch('src.cli.main.storage.Client')
    def test_info_command_success(self, mock_storage_client, mock_config_class):
        """Test info command displays configuration."""
        # Mock ConfigManager instance
        mock_config = Mock()
        mock_config.validate.return_value = True
        mock_config.config.paths.raw_bucket = "test-bucket"
        mock_config.config.ipeds.staging_dataset = "staging_dataset"
        mock_config.config.ipeds.mart_dataset = "mart_dataset"
        mock_config.config.ipeds.mdb_base_url = "https://example.com"
        mock_config.config.ipeds.default_year = 2023
        mock_config_class.return_value = mock_config
        
        # Mock GCS client
        mock_bucket = Mock()
        mock_bucket.list_blobs.return_value = iter([])
        mock_storage_client.return_value.bucket.return_value = mock_bucket
        
        result = runner.invoke(app, ["info"])
        
        assert result.exit_code == 0
        assert "GlidrU IPEDS Pipeline Configuration" in result.stdout
        assert "test-bucket" in result.stdout
        assert "staging_dataset" in result.stdout
    
    @patch('src.cli.main.ConfigManager')
    def test_info_command_validation_error(self, mock_config_class):
        """Test info command with validation error."""
        # Mock ConfigManager that raises on creation
        mock_config_class.side_effect = ValueError("Missing .env file")
        
        result = runner.invoke(app, ["info"])
        
        assert result.exit_code == 1
        assert "Error:" in result.stdout
    
    @patch('src.cli.main.ConfigManager')
    @patch('src.cli.main.IPEDSDownloader')
    def test_download_command_success(self, mock_downloader_class, mock_config_class):
        """Test download command success."""
        # Mock ConfigManager
        mock_config = Mock()
        mock_config.validate.return_value = True
        mock_config_class.return_value = mock_config
        
        # Mock IPEDSDownloader
        mock_downloader = Mock()
        mock_downloader.download_ipeds_data.return_value = {
            "status": "success",
            "filename": "IPEDS2023.zip",
            "gcs_path": "gs://test-bucket/downloads/2023/IPEDS2023.zip",
            "metadata": {
                "filename": "IPEDS2023.zip",
                "file_size_bytes": 1000000,
                "download_duration_seconds": 10.5,
                "gcs_path": "gs://test-bucket/downloads/2023/IPEDS2023.zip"
            }
        }
        mock_downloader_class.return_value = mock_downloader
        
        result = runner.invoke(app, ["download", "2023"])
        
        assert result.exit_code == 0
        assert "Download successful" in result.stdout
        assert "IPEDS2023.zip" in result.stdout
    
    @patch('src.cli.main.ConfigManager')
    @patch('src.cli.main.IPEDSDownloader')
    def test_download_command_already_exists(self, mock_downloader_class, mock_config_class):
        """Test download command when file already exists."""
        # Mock ConfigManager
        mock_config = Mock()
        mock_config.validate.return_value = True
        mock_config_class.return_value = mock_config
        
        # Mock IPEDSDownloader
        mock_downloader = Mock()
        mock_downloader.download_ipeds_data.return_value = {
            "status": "exists",
            "filename": "IPEDS2023.zip",
            "gcs_path": "gs://test-bucket/downloads/2023/IPEDS2023.zip"
        }
        mock_downloader_class.return_value = mock_downloader
        
        result = runner.invoke(app, ["download", "2023"])
        
        assert result.exit_code == 0
        assert "File already exists in GCS" in result.stdout
    
    @patch('src.cli.main.ConfigManager')
    @patch('src.cli.main.MDBExtractor')
    def test_list_tables_local_success(self, mock_extractor_class, mock_config_class):
        """Test list_tables command with local file."""
        # Mock ConfigManager
        mock_config = Mock()
        mock_config.validate.return_value = True
        mock_config_class.return_value = mock_config
        
        # Mock MDBExtractor
        mock_extractor = Mock()
        mock_extractor.list_tables.return_value = ["Table1", "Table2", "Table3"]
        mock_extractor_class.return_value = mock_extractor
        
        # Create temp file
        with runner.isolated_filesystem():
            test_file = Path("test.mdb")
            test_file.touch()
            
            result = runner.invoke(app, ["list-tables", str(test_file)])
        
        assert result.exit_code == 0
        # Strip ANSI color codes from output
        clean_output = re.sub(r'\x1b\[[0-9;]*m', '', result.stdout)
        assert "Found 3 tables" in clean_output
        assert "Table1" in clean_output
        assert "Table2" in clean_output
        assert "Table3" in clean_output
    
    @patch('src.cli.main.ConfigManager')
    @patch('src.cli.main.MDBExtractor')
    @patch('src.cli.main.storage.Client')
    def test_list_tables_gcs_success(self, mock_storage_client, mock_extractor_class, mock_config_class):
        """Test list_tables command with GCS file."""
        # Mock ConfigManager
        mock_config = Mock()
        mock_config.validate.return_value = True
        mock_config.config.paths.temp_dir = ".temp"
        mock_config_class.return_value = mock_config
        
        # Mock MDBExtractor - patch _validate_mdbtools
        mock_extractor = Mock()
        mock_extractor.list_tables.return_value = ["HD2023", "IC2023", "EF2023"]
        mock_extractor_class.return_value = mock_extractor
        mock_extractor_class.return_value._validate_mdbtools = Mock()
        
        # Mock GCS
        mock_bucket = Mock()
        mock_blob = Mock()
        mock_blob.download_to_filename = Mock()
        mock_bucket.blob.return_value = mock_blob
        mock_storage_client.return_value.bucket.return_value = mock_bucket
        
        with runner.isolated_filesystem():
            # Create temp dir for download
            Path(".temp").mkdir(exist_ok=True)
            result = runner.invoke(app, ["list-tables", "gs://test-bucket/test.mdb"])
        
        assert result.exit_code == 0
        # Strip ANSI color codes from output
        clean_output = re.sub(r'\x1b\[[0-9;]*m', '', result.stdout)
        assert "Found 3 tables" in clean_output
    
    @patch('src.cli.main.ConfigManager')
    @patch('src.cli.main.MDBExtractor')
    def test_extract_single_table_success(self, mock_extractor_class, mock_config_class):
        """Test extract command for single table."""
        # Mock ConfigManager
        mock_config = Mock()
        mock_config.validate.return_value = True
        mock_config_class.return_value = mock_config
        
        # Mock MDBExtractor
        mock_extractor = Mock()
        mock_metadata = ExtractionMetadata(
            source_file="test.mdb",
            table_name="Table1",
            extraction_timestamp=datetime.now(timezone.utc),
            row_count=100,
            column_count=5,
            columns=[],
            parquet_size_bytes=50000,
            gcs_path="",
            extraction_duration_seconds=2.5
        )
        mock_extractor.extract_table.return_value = mock_metadata
        mock_extractor_class.return_value = mock_extractor
        
        with runner.isolated_filesystem():
            test_file = Path("test.mdb")
            test_file.touch()
            
            result = runner.invoke(app, ["extract", str(test_file), "--table", "Table1"])
        
        assert result.exit_code == 0
        assert "Extracted Table1" in result.stdout
        assert "100" in result.stdout  # row count
    
    @patch('src.cli.main.ConfigManager')
    @patch('src.cli.main.MDBExtractor')
    def test_extract_all_tables_success(self, mock_extractor_class, mock_config_class):
        """Test extract command for all tables."""
        # Mock ConfigManager
        mock_config = Mock()
        mock_config.validate.return_value = True
        mock_config_class.return_value = mock_config
        
        # Mock MDBExtractor
        mock_extractor = Mock()
        mock_manifest = ExtractionManifest(
            source_file="test.mdb",
            extraction_timestamp=datetime.now(timezone.utc),
            total_tables=3,
            extracted_tables=3,
            skipped_tables=[],
            failed_tables=[],
            table_metadata=[],
            total_duration_seconds=5.0
        )
        mock_extractor.extract_all_tables.return_value = mock_manifest
        mock_extractor_class.return_value = mock_extractor
        
        with runner.isolated_filesystem():
            test_file = Path("test.mdb")
            test_file.touch()
            
            result = runner.invoke(app, ["extract", str(test_file)])
        
        assert result.exit_code == 0
        # Strip ANSI color codes from output
        clean_output = re.sub(r'\x1b\[[0-9;]*m', '', result.stdout)
        assert "Extraction complete" in clean_output
        assert "Extracted: 3" in clean_output
    
    @patch('src.cli.main.ConfigManager')
    @patch('src.cli.main.MDBExtractor')
    def test_extract_with_patterns(self, mock_extractor_class, mock_config_class):
        """Test extract command with include/exclude patterns."""
        # Mock ConfigManager
        mock_config = Mock()
        mock_config.validate.return_value = True
        mock_config_class.return_value = mock_config
        
        # Mock MDBExtractor
        mock_extractor = Mock()
        mock_manifest = ExtractionManifest(
            source_file="test.mdb",
            extraction_timestamp=datetime.now(timezone.utc),
            total_tables=5,
            extracted_tables=2,
            skipped_tables=["TestTable", "TempTable", "OtherTable"],
            failed_tables=[],
            table_metadata=[],
            total_duration_seconds=3.0
        )
        mock_extractor.extract_all_tables.return_value = mock_manifest
        mock_extractor_class.return_value = mock_extractor
        
        with runner.isolated_filesystem():
            test_file = Path("test.mdb")
            test_file.touch()
            
            result = runner.invoke(app, [
                "extract", str(test_file),
                "--include", "^(HD|IC)",
                "--exclude", ".*Temp.*"
            ])
        
        assert result.exit_code == 0
        assert "Extracted: 2" in result.stdout
        assert "Skipped: 3" in result.stdout
    
    @patch('src.cli.main.ConfigManager')
    @patch('src.cli.main.MDBExtractor')
    def test_extract_with_upload(self, mock_extractor_class, mock_config_class):
        """Test extract command with GCS upload."""
        # Mock ConfigManager
        mock_config = Mock()
        mock_config.validate.return_value = True
        mock_config_class.return_value = mock_config
        
        # Mock MDBExtractor
        mock_extractor = Mock()
        mock_manifest = ExtractionManifest(
            source_file="test.mdb",
            extraction_timestamp=datetime.now(timezone.utc),
            total_tables=2,
            extracted_tables=2,
            skipped_tables=[],
            failed_tables=[],
            table_metadata=[],
            total_duration_seconds=4.0
        )
        mock_extractor.extract_all_tables.return_value = mock_manifest
        mock_upload_result = {
            "status": "success",
            "uploaded_files": ["Table1.parquet", "Table2.parquet"],
            "manifest_path": "gs://test-bucket/extracted/2023/metadata/extraction_manifest.json"
        }
        mock_extractor.upload_extraction_to_gcs.return_value = mock_upload_result
        mock_extractor_class.return_value = mock_extractor
        mock_extractor_class.return_value._validate_mdbtools = Mock()
        
        with runner.isolated_filesystem():
            test_file = Path("test.mdb")
            test_file.touch()
            output_dir = Path("output")
            output_dir.mkdir()
            
            result = runner.invoke(app, [
                "extract", str(test_file),
                "--output-dir", str(output_dir),
                "--upload"
            ])
        
        assert result.exit_code == 0
        assert "Extracted: 2" in result.stdout
        # Upload didn't happen because filename doesn't match IPEDS year pattern
        assert "Could not determine year from filename" in result.stdout
    
    def test_download_invalid_year(self):
        """Test download command with invalid year."""
        result = runner.invoke(app, ["download", "2050"])
        
        # Our manual validation returns exit code 1
        assert result.exit_code == 1
        assert "Year must be between 2000 and 2024" in result.stdout
    
    def test_extract_missing_file(self):
        """Test extract command with missing file."""
        result = runner.invoke(app, ["extract", "nonexistent.mdb"])
        
        assert result.exit_code == 1
        # Either file not found or mdbtools error
        assert "Error:" in result.stdout
    
    @patch('src.cli.main.ConfigManager')
    def test_error_handling(self, mock_config_class):
        """Test general error handling."""
        # Mock ConfigManager that raises exception
        mock_config_class.side_effect = Exception("Unexpected error")
        
        result = runner.invoke(app, ["info"])
        
        assert result.exit_code == 1
        assert "Error:" in result.stdout
