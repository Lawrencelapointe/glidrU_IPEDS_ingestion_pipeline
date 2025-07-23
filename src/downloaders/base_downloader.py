# Copyright (c) 2025 Blue Sky Mind LLC
# All Rights Reserved.
# Proprietary and confidential.

"""Base downloader abstract class and metadata models."""

from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field


class DownloadMetadata(BaseModel):
    """Metadata for a downloaded file."""

    filename: str = Field(..., description="Name of the downloaded file")
    source_url: str = Field(..., description="URL the file was downloaded from")
    download_timestamp: datetime = Field(..., description="When the download completed")
    file_size_bytes: int = Field(..., description="Size of the downloaded file in bytes")
    checksum_md5: str = Field(..., description="MD5 checksum of the file")
    checksum_sha256: str = Field(..., description="SHA256 checksum of the file")
    gcs_path: str = Field(..., description="GCS path where file is stored")
    download_duration_seconds: float = Field(..., description="Time taken to download")
    http_status_code: int = Field(..., description="HTTP status code of the download")

    model_config = ConfigDict(
        # datetime fields automatically serialize to ISO format in Pydantic V2
    )


class BaseDownloader(ABC):
    """Abstract base class for file downloaders."""

    @abstractmethod
    def download(self, url: str, destination: Path) -> DownloadMetadata:
        """
        Download a file from a URL to a destination.

        Args:
            url: URL to download from
            destination: Local path to save the file

        Returns:
            DownloadMetadata object with download information
        """
        pass

    @abstractmethod
    def upload_to_gcs(self, local_path: Path, gcs_path: str) -> str:
        """
        Upload a local file to Google Cloud Storage.

        Args:
            local_path: Local file path
            gcs_path: Destination GCS path (without bucket prefix)

        Returns:
            Full GCS URI of the uploaded file
        """
        pass
