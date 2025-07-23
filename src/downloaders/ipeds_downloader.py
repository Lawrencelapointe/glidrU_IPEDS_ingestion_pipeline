# Copyright (c) 2025 Blue Sky Mind LLC
# All Rights Reserved.
# Proprietary and confidential.

"""IPEDS data file downloader implementation."""

import hashlib
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, cast
from urllib.parse import urljoin

import requests
from google.cloud import storage  # type: ignore
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from ..core.config_manager import ConfigManager
from .base_downloader import BaseDownloader, DownloadMetadata

logger = logging.getLogger(__name__)


class IPEDSDownloader(BaseDownloader):
    """Downloads IPEDS Access database files from NCES."""

    def __init__(self, config_manager: ConfigManager):
        """
        Initialize the IPEDS downloader.

        Args:
            config_manager: Configuration manager instance
        """
        self.config = config_manager
        self.storage_client = storage.Client()

        # Get bucket name without gs:// prefix
        bucket_name = self.config.config.paths.raw_bucket.replace('gs://', '').rstrip('/')
        self.bucket = self.storage_client.bucket(bucket_name)

        # Configure requests session with retry logic
        self.session = requests.Session()
        retry_strategy = Retry(
            total=int(self.config._parser.get('downloader', 'retry_attempts', fallback='3')),
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

        # Set timeout from config or default
        self.timeout = int(self.config._parser.get('downloader', 'timeout_seconds', fallback='300'))
        self.chunk_size = int(self.config._parser.get('downloader', 'chunk_size_mb', fallback='10')) * 1024 * 1024

    def build_ipeds_url(self, year: int, version: str = "final") -> str:
        """
        Build the URL for an IPEDS data file.

        Args:
            year: Academic year (e.g., 2023)
            version: Data version - "final", "provisional", or "revised"

        Returns:
            Complete URL to the IPEDS zip file
        """
        base_url = self.config.config.ipeds.mdb_base_url

        # Determine suffix based on version
        suffix = ""
        if version.lower() == "provisional":
            suffix = "_pv"
        elif version.lower() == "revised":
            suffix = "_rv"

        filename = f"IPEDS{year}{suffix}.zip"
        return urljoin(base_url + "/", filename)

    def download(self, url: str, destination: Path) -> DownloadMetadata:
        """
        Download a file from a URL with resume capability.

        Args:
            url: URL to download from
            destination: Local path to save the file

        Returns:
            DownloadMetadata object with download information

        Raises:
            requests.RequestException: If download fails after all retries
        """
        start_time = time.time()
        destination = Path(destination)
        destination.parent.mkdir(parents=True, exist_ok=True)

        # Check if partial download exists
        partial_path = destination.with_suffix(destination.suffix + '.partial')
        resume_pos = 0

        if partial_path.exists():
            resume_pos = partial_path.stat().st_size
            logger.info(f"Resuming download from byte {resume_pos}")

        # Prepare headers for resume
        headers = {}
        if resume_pos > 0:
            headers['Range'] = f'bytes={resume_pos}-'

        try:
            # Make the request
            logger.info(f"Downloading {url} to {destination}")
            response = self.session.get(url, headers=headers, stream=True, timeout=self.timeout)
            response.raise_for_status()

            # Get total file size
            total_size = int(response.headers.get('content-length', 0))
            if resume_pos > 0:
                total_size += resume_pos

            # Download with progress tracking
            mode = 'ab' if resume_pos > 0 else 'wb'
            with open(partial_path, mode) as f:
                downloaded = resume_pos
                for chunk in response.iter_content(chunk_size=self.chunk_size):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)

                        # Log progress every 10%
                        if total_size > 0:
                            progress = (downloaded / total_size) * 100
                            if int(progress) % 10 == 0 and int(progress) != int((downloaded - len(chunk)) / total_size * 100):
                                logger.info(f"Download progress: {progress:.1f}%")

            # Move partial to final destination
            partial_path.rename(destination)

            # Calculate checksums
            md5_hash = hashlib.md5()
            sha256_hash = hashlib.sha256()

            with open(destination, 'rb') as f:
                for chunk in iter(lambda: f.read(self.chunk_size), b''):
                    md5_hash.update(chunk)
                    sha256_hash.update(chunk)

            # Create metadata
            metadata = DownloadMetadata(
                filename=destination.name,
                source_url=url,
                download_timestamp=datetime.now(timezone.utc),
                file_size_bytes=destination.stat().st_size,
                checksum_md5=md5_hash.hexdigest(),
                checksum_sha256=sha256_hash.hexdigest(),
                gcs_path="",  # Will be set after upload
                download_duration_seconds=time.time() - start_time,
                http_status_code=response.status_code
            )

            logger.info(f"Download completed: {destination.name} ({metadata.file_size_bytes:,} bytes)")
            return metadata

        except requests.RequestException as e:
            logger.error(f"Download failed: {str(e)}")
            # Clean up partial file on error
            if partial_path.exists():
                partial_path.unlink()
            raise

    def upload_to_gcs(self, local_path: Path, gcs_path: str) -> str:
        """
        Upload a local file to Google Cloud Storage.

        Args:
            local_path: Local file path
            gcs_path: Destination GCS path (without bucket prefix)

        Returns:
            Full GCS URI of the uploaded file
        """
        logger.info(f"Uploading {local_path} to gs://{self.bucket.name}/{gcs_path}")

        blob = self.bucket.blob(gcs_path)

        # Use resumable upload for large files
        blob.upload_from_filename(str(local_path), timeout=self.timeout)

        gcs_uri = f"gs://{self.bucket.name}/{gcs_path}"
        logger.info(f"Upload completed: {gcs_uri}")

        return gcs_uri

    def download_ipeds_data(self, year: int, version: str = "final", force: bool = False) -> Dict[str, Any]:
        """
        Download IPEDS data for a specific year and version.

        Args:
            year: Academic year to download
            version: Data version - "final", "provisional", or "revised"
            force: Force re-download even if file exists in GCS

        Returns:
            Dictionary with download results and metadata
        """
        # Build paths
        filename = f"IPEDS{year}{'_pv' if version == 'provisional' else '_rv' if version == 'revised' else ''}.zip"
        gcs_path = f"downloads/{year}/{filename}"
        metadata_path = f"downloads/{year}/metadata.json"

        # Check if already exists in GCS
        blob = self.bucket.blob(gcs_path)
        if blob.exists() and not force:
            logger.info(f"File already exists in GCS: {gcs_path}")

            # Load existing metadata
            metadata_blob = self.bucket.blob(metadata_path)
            if metadata_blob.exists():
                metadata_json = metadata_blob.download_as_text()
                return cast(Dict[str, Any], json.loads(metadata_json))
            else:
                return {"status": "exists", "gcs_path": f"gs://{self.bucket.name}/{gcs_path}"}

        # Create temporary directory
        temp_dir = Path(self.config.config.paths.temp_dir) / f"ipeds_{year}"
        temp_dir.mkdir(parents=True, exist_ok=True)

        try:
            # Build URL and download
            url = self.build_ipeds_url(year, version)
            local_path = temp_dir / filename

            # Download the file
            metadata = self.download(url, local_path)

            # Upload to GCS
            gcs_uri = self.upload_to_gcs(local_path, gcs_path)
            metadata.gcs_path = gcs_uri

            # Save metadata to GCS
            metadata_dict = metadata.model_dump()
            metadata_blob = self.bucket.blob(metadata_path)
            metadata_blob.upload_from_string(
                json.dumps(metadata_dict, indent=2, default=str),
                content_type="application/json"
            )

            # Clean up local file
            local_path.unlink()

            return {
                "status": "success",
                "metadata": metadata_dict,
                "gcs_path": gcs_uri
            }

        except Exception as e:
            logger.error(f"Failed to download IPEDS data for year {year}: {str(e)}")
            raise
        finally:
            # Clean up temp directory if empty
            if temp_dir.exists() and not any(temp_dir.iterdir()):
                temp_dir.rmdir()
