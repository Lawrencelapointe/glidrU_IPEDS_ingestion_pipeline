# Copyright (c) 2025 Blue Sky Mind LLC
# All Rights Reserved.
# Proprietary and confidential.

"""Microsoft Access database extractor using mdbtools."""

import json
import logging
import re
import subprocess
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import pyarrow as pa  # type: ignore
import pyarrow.parquet as pq  # type: ignore
from google.cloud import storage  # type: ignore

from ..core.config_manager import ConfigManager
from .extraction_models import ColumnInfo, ExtractionManifest, ExtractionMetadata

logger = logging.getLogger(__name__)


class MDBExtractor:
    """Extracts tables from Microsoft Access databases using mdbtools."""

    def __init__(self, config_manager: ConfigManager):
        """
        Initialize the MDB extractor.

        Args:
            config_manager: Configuration manager instance
        """
        self.config = config_manager
        self._validate_mdbtools()

        # Initialize GCS client
        self.storage_client = storage.Client()
        bucket_name = self.config.config.paths.raw_bucket.replace('gs://', '').rstrip('/')
        self.bucket = self.storage_client.bucket(bucket_name)

        # Get configuration values
        self.compression = self.config._parser.get('extractor', 'default_compression', fallback='snappy')
        self.max_table_size_gb = float(self.config._parser.get('extractor', 'max_table_size_gb', fallback='5'))

    def _validate_mdbtools(self) -> None:
        """Ensure mdbtools is installed and working."""
        try:
            result = subprocess.run(['mdb-ver'], check=True, capture_output=True, text=True)
            logger.debug(f"mdbtools version: {result.stdout.strip()}")
        except (subprocess.CalledProcessError, FileNotFoundError):
            raise RuntimeError("mdbtools not found. Please install mdbtools.")

    def list_tables(self, mdb_path: Path) -> List[str]:
        """
        List all tables in the MDB file.

        Args:
            mdb_path: Path to the MDB file

        Returns:
            List of table names
        """
        if not mdb_path.exists():
            raise FileNotFoundError(f"MDB file not found: {mdb_path}")

        try:
            # Use mdb-tables with -1 flag for one table per line
            result = subprocess.run(
                ['mdb-tables', '-1', str(mdb_path)],
                check=True,
                capture_output=True,
                text=True
            )

            # Parse output and filter empty lines
            tables = [t.strip() for t in result.stdout.splitlines() if t.strip()]
            logger.info(f"Found {len(tables)} tables in {mdb_path.name}")

            return tables

        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to list tables: {e.stderr}")
            raise RuntimeError(f"Failed to list tables from {mdb_path}: {e.stderr}")

    def _infer_column_types(self, df: pd.DataFrame) -> List[ColumnInfo]:
        """
        Infer column types from a pandas DataFrame.

        Args:
            df: DataFrame to analyze

        Returns:
            List of ColumnInfo objects
        """
        columns = []

        for col in df.columns:
            dtype = str(df[col].dtype)
            nullable = bool(df[col].isna().any())

            # Map pandas types to more generic types
            if 'int' in dtype:
                data_type = 'integer'
            elif 'float' in dtype:
                data_type = 'float'
            elif 'bool' in dtype:
                data_type = 'boolean'
            elif 'datetime' in dtype:
                data_type = 'timestamp'
            elif 'object' in dtype:
                # Check if it might be a date string
                sample = df[col].dropna().head(10)
                if len(sample) > 0 and all(self._is_date_string(str(v)) for v in sample):
                    data_type = 'date'
                else:
                    data_type = 'string'
            else:
                data_type = 'string'

            columns.append(ColumnInfo(
                name=col,
                data_type=data_type,
                nullable=nullable
            ))

        return columns

    def _is_date_string(self, value: str) -> bool:
        """Check if a string looks like a date."""
        date_patterns = [
            r'^\d{4}-\d{2}-\d{2}$',  # YYYY-MM-DD
            r'^\d{2}/\d{2}/\d{4}$',  # MM/DD/YYYY
            r'^\d{2}-\d{2}-\d{4}$',  # MM-DD-YYYY
        ]
        return any(re.match(pattern, value) for pattern in date_patterns)

    def _clean_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean column names to be Parquet-friendly."""
        # Replace spaces and special characters with underscores
        df.columns = [re.sub(r'[^\w]', '_', col).strip('_') for col in df.columns]
        # Remove duplicate underscores
        df.columns = [re.sub(r'_+', '_', col) for col in df.columns]
        return df

    def extract_table(self,
                     mdb_path: Path,
                     table_name: str,
                     output_path: Optional[Path] = None) -> ExtractionMetadata:
        """
        Extract a single table to Parquet format.

        Args:
            mdb_path: Path to the MDB file
            table_name: Name of the table to extract
            output_path: Optional output path for the Parquet file

        Returns:
            ExtractionMetadata for the extracted table
        """
        start_time = time.time()

        if not mdb_path.exists():
            raise FileNotFoundError(f"MDB file not found: {mdb_path}")

        # Create temporary directory if no output path specified
        if output_path is None:
            temp_dir = Path(self.config.config.paths.temp_dir) / "extraction"
            temp_dir.mkdir(parents=True, exist_ok=True)
            output_path = temp_dir / f"{table_name}.parquet"

        output_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            logger.info(f"Extracting table '{table_name}' from {mdb_path.name}")

            # Export table to CSV using mdb-export
            result = subprocess.run(
                ['mdb-export', str(mdb_path), table_name],
                check=True,
                capture_output=True,
                text=True
            )

            # Check if we got any data
            if not result.stdout.strip():
                raise ValueError(f"Table '{table_name}' appears to be empty")

            # Read CSV data into pandas
            from io import StringIO
            df = pd.read_csv(StringIO(result.stdout))

            # Clean column names
            df = self._clean_column_names(df)

            # Infer column types
            column_info = self._infer_column_types(df)

            # Convert date strings to datetime where appropriate
            for col_info in column_info:
                if col_info.data_type == 'date':
                    try:
                        df[col_info.name] = pd.to_datetime(df[col_info.name], errors='coerce')
                    except Exception as e:
                        logger.warning(f"Failed to convert column {col_info.name} to datetime: {e}")

            # Save as Parquet
            table = pa.Table.from_pandas(df)
            pq.write_table(
                table,
                str(output_path),
                compression=self.compression
            )

            # Get file size
            file_size = output_path.stat().st_size

            # Create metadata
            metadata = ExtractionMetadata(
                source_file=str(mdb_path),
                table_name=table_name,
                extraction_timestamp=pd.Timestamp.utcnow(),
                row_count=len(df),
                column_count=len(df.columns),
                columns=column_info,
                parquet_size_bytes=file_size,
                gcs_path="",  # Will be set if uploaded
                extraction_duration_seconds=time.time() - start_time
            )

            logger.info(f"Extracted {table_name}: {len(df)} rows, {len(df.columns)} columns")

            return metadata

        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to extract table '{table_name}': {e.stderr}")
            raise RuntimeError(f"Failed to extract table '{table_name}': {e.stderr}")
        except Exception as e:
            logger.error(f"Error extracting table '{table_name}': {str(e)}")
            raise

    def extract_all_tables(self,
                          mdb_path: Path,
                          include_pattern: Optional[str] = None,
                          exclude_pattern: Optional[str] = None,
                          output_dir: Optional[Path] = None) -> ExtractionManifest:
        """
        Extract all tables matching criteria.

        Args:
            mdb_path: Path to the MDB file
            include_pattern: Optional regex pattern for tables to include
            exclude_pattern: Optional regex pattern for tables to exclude
            output_dir: Optional output directory for Parquet files

        Returns:
            ExtractionManifest with results
        """
        start_time = time.time()

        # List all tables
        all_tables = self.list_tables(mdb_path)

        # Filter tables based on patterns
        tables_to_extract = []
        skipped_tables = []

        for table in all_tables:
            # Check include pattern
            if include_pattern and not re.match(include_pattern, table):
                skipped_tables.append(table)
                continue

            # Check exclude pattern
            if exclude_pattern and re.match(exclude_pattern, table):
                skipped_tables.append(table)
                continue

            tables_to_extract.append(table)

        logger.info(f"Extracting {len(tables_to_extract)} tables, skipping {len(skipped_tables)}")

        # Create output directory
        if output_dir is None:
            temp_dir = Path(self.config.config.paths.temp_dir) / "extraction" / mdb_path.stem
            temp_dir.mkdir(parents=True, exist_ok=True)
            output_dir = temp_dir
        else:
            output_dir.mkdir(parents=True, exist_ok=True)

        # Extract each table
        table_metadata = []
        failed_tables = []

        for table in tables_to_extract:
            try:
                output_path = output_dir / f"{table}.parquet"
                metadata = self.extract_table(mdb_path, table, output_path)
                table_metadata.append(metadata)
            except Exception as e:
                logger.error(f"Failed to extract table '{table}': {str(e)}")
                failed_tables.append(table)

        # Create manifest
        manifest = ExtractionManifest(
            source_file=str(mdb_path),
            extraction_timestamp=pd.Timestamp.utcnow(),
            total_tables=len(all_tables),
            extracted_tables=len(table_metadata),
            skipped_tables=skipped_tables,
            failed_tables=failed_tables,
            table_metadata=table_metadata,
            total_duration_seconds=time.time() - start_time
        )

        logger.info(f"Extraction complete: {len(table_metadata)} tables extracted, "
                   f"{len(failed_tables)} failed, {len(skipped_tables)} skipped")

        return manifest

    def upload_extraction_to_gcs(self,
                                local_dir: Path,
                                year: int,
                                manifest: ExtractionManifest) -> Dict[str, Any]:
        """
        Upload extracted Parquet files and manifest to GCS.

        Args:
            local_dir: Directory containing Parquet files
            year: Academic year for organization
            manifest: Extraction manifest

        Returns:
            Dictionary with upload results
        """
        logger.info(f"Uploading extraction results to GCS for year {year}")

        uploaded_files = []

        # Upload each Parquet file
        for metadata in manifest.table_metadata:
            local_path = local_dir / f"{metadata.table_name}.parquet"
            if local_path.exists():
                gcs_path = f"extracted/{year}/tables/{metadata.table_name}.parquet"
                blob = self.bucket.blob(gcs_path)
                blob.upload_from_filename(str(local_path))

                # Update metadata with GCS path
                metadata.gcs_path = f"gs://{self.bucket.name}/{gcs_path}"
                uploaded_files.append(metadata.gcs_path)

                logger.debug(f"Uploaded {metadata.table_name}.parquet to {gcs_path}")

        # Save manifest to GCS
        manifest_path = f"extracted/{year}/metadata/extraction_manifest.json"
        manifest_blob = self.bucket.blob(manifest_path)
        manifest_blob.upload_from_string(
            json.dumps(manifest.model_dump(), indent=2, default=str),
            content_type="application/json"
        )

        logger.info(f"Uploaded {len(uploaded_files)} files and manifest to GCS")

        return {
            "status": "success",
            "uploaded_files": uploaded_files,
            "manifest_path": f"gs://{self.bucket.name}/{manifest_path}"
        }
