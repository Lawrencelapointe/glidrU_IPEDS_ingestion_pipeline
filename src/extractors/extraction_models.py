# Copyright (c) 2025 Blue Sky Mind LLC
# All Rights Reserved.
# Proprietary and confidential.

"""Data models for extraction metadata."""

from datetime import datetime
from typing import List

from pydantic import BaseModel, ConfigDict, Field


class ColumnInfo(BaseModel):
    """Information about a table column."""

    name: str = Field(..., description="Column name")
    data_type: str = Field(..., description="Data type (as detected)")
    nullable: bool = Field(True, description="Whether column allows nulls")


class ExtractionMetadata(BaseModel):
    """Metadata for an extracted table."""

    source_file: str = Field(..., description="Source MDB file path")
    table_name: str = Field(..., description="Name of the extracted table")
    extraction_timestamp: datetime = Field(..., description="When extraction occurred")
    row_count: int = Field(..., description="Number of rows in the table")
    column_count: int = Field(..., description="Number of columns in the table")
    columns: List[ColumnInfo] = Field(..., description="Column information")
    parquet_size_bytes: int = Field(..., description="Size of the Parquet file")
    gcs_path: str = Field(..., description="GCS path where Parquet is stored")
    extraction_duration_seconds: float = Field(..., description="Time taken to extract")

    model_config = ConfigDict(
        # datetime fields automatically serialize to ISO format in Pydantic V2
    )


class ExtractionManifest(BaseModel):
    """Manifest for all tables extracted from a single MDB file."""

    source_file: str = Field(..., description="Source MDB file path")
    extraction_timestamp: datetime = Field(..., description="When extraction started")
    total_tables: int = Field(..., description="Total number of tables in MDB")
    extracted_tables: int = Field(..., description="Number of tables extracted")
    skipped_tables: List[str] = Field(default_factory=list, description="Tables that were skipped")
    failed_tables: List[str] = Field(default_factory=list, description="Tables that failed to extract")
    table_metadata: List[ExtractionMetadata] = Field(..., description="Metadata for each extracted table")
    total_duration_seconds: float = Field(..., description="Total extraction time")

    model_config = ConfigDict(
        # datetime fields automatically serialize to ISO format in Pydantic V2
    )
