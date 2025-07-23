# Copyright (c) 2025 Blue Sky Mind LLC
# All Rights Reserved.
# Proprietary and confidential.

"""Unit tests for IPEDSDownloader."""

import json
import time
from datetime import datetime
from pathlib import Path
from unittest.mock import Mock, MagicMock, patch, PropertyMock

import pytest
import requests
from google.cloud import storage

from src.core.config_manager import ConfigManager
from src.downloaders.ipeds_downloader import IPEDSDownloader, DownloadMetadata


@pytest.fixture
def mock_config():
    """Create a mock ConfigManager."""
    config = Mock(spec=ConfigManager)
    
    # Mock config structure
    config.config.paths.raw_bucket = "gs://test-bucket"
    config.config.paths.temp_dir = "/tmp/test"
    config.config.ipeds.mdb_base_url = "https://example.com/ipeds"
    config.config.ipeds.default_year = 2024
    
    # Mock parser for downloader settings
    config._parser = Mock()
    config._parser.get.side_effect = lambda section, key, fallback=None: {
        ('downloader', 'retry_attempts'): '3',
        ('downloader', 'timeout_seconds'): '300',
        ('downloader', 'chunk_size_mb'): '10',
        ('downloader', 'retry_delay_seconds'): '5'
    }.get((section, key), fallback)
    
    return config


@pytest.fixture
def mock_storage_client():
    """Create a mock Google Cloud Storage client."""
    with patch('src.downloaders.ipeds_downloader.storage.Client') as mock_client:
        # Mock bucket and blob
        mock_bucket = Mock()
        mock_blob = Mock()
        
        mock_client.return_value.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob
        mock_bucket.name = "test-bucket"
        
        yield mock_client, mock_bucket, mock_blob


class TestIPEDSDownloader:
    """Test cases for IPEDSDownloader."""
    
    def test_init(self, mock_config, mock_storage_client):
        """Test downloader initialization."""
        mock_client, mock_bucket, _ = mock_storage_client
        
        downloader = IPEDSDownloader(mock_config)
        
        assert downloader.config == mock_config
        assert downloader.timeout == 300
        assert downloader.chunk_size == 10 * 1024 * 1024
        mock_client.return_value.bucket.assert_called_once_with("test-bucket")
    
    def test_build_ipeds_url_final(self, mock_config, mock_storage_client):
        """Test building URL for final version."""
        downloader = IPEDSDownloader(mock_config)
        
        url = downloader.build_ipeds_url(2023, "final")
        assert url == "https://example.com/ipeds/IPEDS2023.zip"
    
    def test_build_ipeds_url_provisional(self, mock_config, mock_storage_client):
        """Test building URL for provisional version."""
        downloader = IPEDSDownloader(mock_config)
        
        url = downloader.build_ipeds_url(2023, "provisional")
        assert url == "https://example.com/ipeds/IPEDS2023_pv.zip"
    
    def test_build_ipeds_url_revised(self, mock_config, mock_storage_client):
        """Test building URL for revised version."""
        downloader = IPEDSDownloader(mock_config)
        
        url = downloader.build_ipeds_url(2023, "revised")
        assert url == "https://example.com/ipeds/IPEDS2023_rv.zip"
    
    @patch('src.downloaders.ipeds_downloader.requests.Session')
    def test_download_success(self, mock_session, mock_config, mock_storage_client, tmp_path):
        """Test successful file download."""
        # Setup mock response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.headers = {'content-length': '1000'}
        mock_response.iter_content.return_value = [b'test data chunk']
        mock_response.raise_for_status.return_value = None
        
        mock_session.return_value.get.return_value = mock_response
        
        downloader = IPEDSDownloader(mock_config)
        downloader.session = mock_session.return_value
        
        # Test download
        dest_path = tmp_path / "test.zip"
        metadata = downloader.download("https://example.com/file.zip", dest_path)
        
        # Verify
        assert dest_path.exists()
        assert metadata.filename == "test.zip"
        assert metadata.source_url == "https://example.com/file.zip"
        assert metadata.file_size_bytes > 0
        assert metadata.checksum_md5 != ""
        assert metadata.checksum_sha256 != ""
        assert metadata.download_duration_seconds > 0
        assert metadata.http_status_code == 200
    
    @patch('src.downloaders.ipeds_downloader.requests.Session')
    def test_download_resume(self, mock_session, mock_config, mock_storage_client, tmp_path):
        """Test resuming a partial download."""
        # Create partial file
        partial_path = tmp_path / "test.zip.partial"
        partial_path.write_bytes(b'existing data')
        
        # Setup mock response for resumed download
        mock_response = Mock()
        mock_response.status_code = 206  # Partial content
        mock_response.headers = {'content-length': '500'}
        mock_response.iter_content.return_value = [b'new data chunk']
        mock_response.raise_for_status.return_value = None
        
        mock_session.return_value.get.return_value = mock_response
        
        downloader = IPEDSDownloader(mock_config)
        downloader.session = mock_session.return_value
        
        # Test download
        dest_path = tmp_path / "test.zip"
        metadata = downloader.download("https://example.com/file.zip", dest_path)
        
        # Verify resume headers were used
        mock_session.return_value.get.assert_called_once()
        call_args = mock_session.return_value.get.call_args
        assert 'Range' in call_args[1]['headers']
        assert call_args[1]['headers']['Range'] == 'bytes=13-'
    
    @patch('src.downloaders.ipeds_downloader.requests.Session')
    def test_download_failure(self, mock_session, mock_config, mock_storage_client, tmp_path):
        """Test download failure handling."""
        # Setup mock to raise exception
        mock_session.return_value.get.side_effect = requests.RequestException("Network error")
        
        downloader = IPEDSDownloader(mock_config)
        downloader.session = mock_session.return_value
        
        # Test download
        dest_path = tmp_path / "test.zip"
        with pytest.raises(requests.RequestException):
            downloader.download("https://example.com/file.zip", dest_path)
        
        # Verify partial file was cleaned up
        partial_path = dest_path.with_suffix(dest_path.suffix + '.partial')
        assert not partial_path.exists()
    
    def test_upload_to_gcs(self, mock_config, mock_storage_client, tmp_path):
        """Test uploading file to GCS."""
        _, mock_bucket, mock_blob = mock_storage_client
        
        downloader = IPEDSDownloader(mock_config)
        
        # Create temp directory structure
        temp_dir = tmp_path / "temp"
        temp_dir.mkdir(parents=True)
        
        # Create test file
        test_file = temp_dir / "test.zip"
        test_file.write_bytes(b'test content')
        
        # Test upload
        gcs_uri = downloader.upload_to_gcs(test_file, "path/to/file.zip")
        
        # Verify
        assert gcs_uri == "gs://test-bucket/path/to/file.zip"
        mock_bucket.blob.assert_called_once_with("path/to/file.zip")
        mock_blob.upload_from_filename.assert_called_once_with(str(test_file), timeout=300)
    
    @patch.object(IPEDSDownloader, 'download')
    @patch.object(IPEDSDownloader, 'upload_to_gcs')
    def test_download_ipeds_data_success(self, mock_upload, mock_download, mock_config, mock_storage_client, tmp_path):
        """Test successful download and upload to GCS."""
        _, mock_bucket, mock_blob = mock_storage_client
        
        # Update config to use tmp_path
        mock_config.config.paths.temp_dir = str(tmp_path)
        
        # Create temp directory structure and file
        ipeds_dir = tmp_path / "ipeds_2023"
        ipeds_dir.mkdir(parents=True)
        test_file = ipeds_dir / "IPEDS2023.zip"
        test_file.write_bytes(b'test content')
        
        # Setup mocks
        mock_blob.exists.return_value = False
        
        mock_metadata = DownloadMetadata(
            filename="IPEDS2023.zip",
            source_url="https://example.com/IPEDS2023.zip",
            download_timestamp=datetime.utcnow(),
            file_size_bytes=1000000,
            checksum_md5="abc123",
            checksum_sha256="def456",
            gcs_path="",
            download_duration_seconds=10.5,
            http_status_code=200
        )
        mock_download.return_value = mock_metadata
        mock_upload.return_value = "gs://test-bucket/downloads/2023/IPEDS2023.zip"
        
        downloader = IPEDSDownloader(mock_config)
        
        # Test download
        result = downloader.download_ipeds_data(2023, "final")
        
        # Verify
        assert result["status"] == "success"
        assert result["gcs_path"] == "gs://test-bucket/downloads/2023/IPEDS2023.zip"
        assert "metadata" in result
        
        # Verify metadata was saved
        mock_bucket.blob.assert_any_call("downloads/2023/metadata.json")
    
    def test_download_ipeds_data_exists(self, mock_config, mock_storage_client):
        """Test handling when file already exists in GCS."""
        _, mock_bucket, mock_blob = mock_storage_client
        
        # Setup mock - file exists
        mock_blob.exists.return_value = True
        
        # Mock metadata blob
        mock_metadata_blob = Mock()
        mock_metadata_blob.exists.return_value = True
        mock_metadata_blob.download_as_text.return_value = json.dumps({
            "filename": "IPEDS2023.zip",
            "file_size_bytes": 1000000
        })
        
        def blob_side_effect(path):
            if path == "downloads/2023/metadata.json":
                return mock_metadata_blob
            return mock_blob
        
        mock_bucket.blob.side_effect = blob_side_effect
        
        downloader = IPEDSDownloader(mock_config)
        
        # Test download without force
        result = downloader.download_ipeds_data(2023, "final", force=False)
        
        # Verify
        assert "filename" in result
        assert result["filename"] == "IPEDS2023.zip"
    
    @patch.object(IPEDSDownloader, 'download')
    @patch.object(IPEDSDownloader, 'upload_to_gcs')
    def test_download_ipeds_data_force(self, mock_upload, mock_download, mock_config, mock_storage_client, tmp_path):
        """Test force re-download."""
        _, mock_bucket, mock_blob = mock_storage_client
        
        # Update config to use tmp_path
        mock_config.config.paths.temp_dir = str(tmp_path)
        
        # Create temp directory structure and file
        ipeds_dir = tmp_path / "ipeds_2023"
        ipeds_dir.mkdir(parents=True)
        test_file = ipeds_dir / "IPEDS2023.zip"
        test_file.write_bytes(b'test content')
        
        # Setup mocks - file exists but force=True
        mock_blob.exists.return_value = True
        
        mock_metadata = DownloadMetadata(
            filename="IPEDS2023.zip",
            source_url="https://example.com/IPEDS2023.zip",
            download_timestamp=datetime.utcnow(),
            file_size_bytes=1000000,
            checksum_md5="abc123",
            checksum_sha256="def456",
            gcs_path="",
            download_duration_seconds=10.5,
            http_status_code=200
        )
        mock_download.return_value = mock_metadata
        mock_upload.return_value = "gs://test-bucket/downloads/2023/IPEDS2023.zip"
        
        downloader = IPEDSDownloader(mock_config)
        
        # Test download with force
        result = downloader.download_ipeds_data(2023, "final", force=True)
        
        # Verify download was called despite file existing
        assert result["status"] == "success"
        mock_download.assert_called_once()
        mock_upload.assert_called_once()
    
    @patch.object(IPEDSDownloader, 'download')
    def test_download_ipeds_data_failure(self, mock_download, mock_config, mock_storage_client):
        """Test handling download failure."""
        _, mock_bucket, mock_blob = mock_storage_client
        
        # Setup mocks
        mock_blob.exists.return_value = False
        mock_download.side_effect = Exception("Download failed")
        
        downloader = IPEDSDownloader(mock_config)
        
        # Test download
        with pytest.raises(Exception, match="Download failed"):
            downloader.download_ipeds_data(2023, "final")
