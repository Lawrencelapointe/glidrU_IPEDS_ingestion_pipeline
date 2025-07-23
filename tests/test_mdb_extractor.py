# Copyright (c) 2025 Blue Sky Mind LLC
# All Rights Reserved.
# Proprietary and confidential.

"""Unit tests for MDBExtractor."""

import json
import subprocess
from datetime import datetime
from pathlib import Path
from unittest.mock import Mock, MagicMock, patch, call
from io import StringIO

import pandas as pd
import pytest
from google.cloud import storage

from src.core.config_manager import ConfigManager
from src.extractors.mdb_extractor import MDBExtractor
from src.extractors.extraction_models import ExtractionMetadata, ExtractionManifest, ColumnInfo


@pytest.fixture
def mock_config():
    """Create a mock ConfigManager."""
    config = Mock(spec=ConfigManager)
    
    # Mock config structure
    config.config.paths.raw_bucket = "gs://test-bucket"
    config.config.paths.temp_dir = "/tmp/test"
    
    # Mock parser for extractor settings
    config._parser = Mock()
    config._parser.get.side_effect = lambda section, key, fallback=None: {
        ('extractor', 'default_compression'): 'snappy',
        ('extractor', 'max_table_size_gb'): '5'
    }.get((section, key), fallback)
    
    return config


@pytest.fixture
def mock_storage_client():
    """Create a mock Google Cloud Storage client."""
    with patch('src.extractors.mdb_extractor.storage.Client') as mock_client:
        # Mock bucket and blob
        mock_bucket = Mock()
        mock_blob = Mock()
        
        mock_client.return_value.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob
        mock_bucket.name = "test-bucket"
        
        yield mock_client, mock_bucket, mock_blob


@pytest.fixture
def sample_csv_data():
    """Sample CSV data that mdb-export might return."""
    return """ID,Name,Value,Date
1,"Test Item",100.50,2023-01-15
2,"Another Item",200.75,2023-02-20
3,"Third Item",300.25,2023-03-25"""


class TestMDBExtractor:
    """Test cases for MDBExtractor."""
    
    @patch('subprocess.run')
    def test_init_success(self, mock_subprocess, mock_config, mock_storage_client):
        """Test successful initialization with mdbtools available."""
        # Mock mdb-ver success
        mock_subprocess.return_value = Mock(
            stdout="mdbtools version 0.9.3",
            stderr="",
            returncode=0
        )
        
        extractor = MDBExtractor(mock_config)
        
        assert extractor.config == mock_config
        assert extractor.compression == "snappy"
        assert extractor.max_table_size_gb == 5.0
        mock_subprocess.assert_called_once_with(
            ['mdb-ver'], check=True, capture_output=True, text=True
        )
    
    @patch('subprocess.run')
    def test_init_mdbtools_not_found(self, mock_subprocess, mock_config, mock_storage_client):
        """Test initialization failure when mdbtools is not available."""
        # Mock mdb-ver failure
        mock_subprocess.side_effect = FileNotFoundError()
        
        with pytest.raises(RuntimeError, match="mdbtools not found"):
            MDBExtractor(mock_config)
    
    @patch('subprocess.run')
    def test_list_tables_success(self, mock_subprocess, mock_config, mock_storage_client, tmp_path):
        """Test listing tables from MDB file."""
        # Create test MDB file
        mdb_file = tmp_path / "test.mdb"
        mdb_file.touch()
        
        # Mock mdb-ver for init
        mock_subprocess.side_effect = [
            Mock(stdout="mdbtools version 0.9.3", stderr="", returncode=0),  # mdb-ver
            Mock(stdout="Table1\nTable2\nTable3\n", stderr="", returncode=0)  # mdb-tables
        ]
        
        extractor = MDBExtractor(mock_config)
        tables = extractor.list_tables(mdb_file)
        
        assert tables == ["Table1", "Table2", "Table3"]
        
        # Verify mdb-tables was called correctly
        calls = mock_subprocess.call_args_list
        assert calls[1][0][0] == ['mdb-tables', '-1', str(mdb_file)]
    
    @patch('subprocess.run')
    def test_list_tables_file_not_found(self, mock_subprocess, mock_config, mock_storage_client, tmp_path):
        """Test listing tables with non-existent file."""
        # Mock mdb-ver for init
        mock_subprocess.return_value = Mock(stdout="mdbtools version 0.9.3", stderr="", returncode=0)
        
        extractor = MDBExtractor(mock_config)
        
        non_existent = tmp_path / "does_not_exist.mdb"
        with pytest.raises(FileNotFoundError):
            extractor.list_tables(non_existent)
    
    @patch('subprocess.run')
    def test_list_tables_mdb_error(self, mock_subprocess, mock_config, mock_storage_client, tmp_path):
        """Test handling mdb-tables error."""
        # Create test MDB file
        mdb_file = tmp_path / "test.mdb"
        mdb_file.touch()
        
        # Mock mdb-ver for init, then mdb-tables failure
        mock_subprocess.side_effect = [
            Mock(stdout="mdbtools version 0.9.3", stderr="", returncode=0),  # mdb-ver
            subprocess.CalledProcessError(1, 'mdb-tables', stderr="Invalid file format")
        ]
        
        extractor = MDBExtractor(mock_config)
        
        with pytest.raises(RuntimeError, match="Failed to list tables"):
            extractor.list_tables(mdb_file)
    
    def test_infer_column_types(self, mock_config, mock_storage_client):
        """Test column type inference."""
        with patch('subprocess.run') as mock_subprocess:
            mock_subprocess.return_value = Mock(stdout="mdbtools version 0.9.3", stderr="", returncode=0)
            extractor = MDBExtractor(mock_config)
        
        # Create test DataFrame
        df = pd.DataFrame({
            'int_col': [1, 2, 3],
            'float_col': [1.1, 2.2, 3.3],
            'str_col': ['a', 'b', 'c'],
            'date_col': ['2023-01-01', '2023-01-02', '2023-01-03'],
            'bool_col': [True, False, True],
            'mixed_col': [1, 'a', None]
        })
        
        columns = extractor._infer_column_types(df)
        
        assert len(columns) == 6
        assert columns[0].name == 'int_col'
        assert columns[0].data_type == 'integer'
        assert columns[1].data_type == 'float'
        assert columns[2].data_type == 'string'
        assert columns[3].data_type == 'date'
        assert columns[4].data_type == 'boolean'
        assert columns[5].data_type == 'string'  # mixed type defaults to string
    
    def test_is_date_string(self, mock_config, mock_storage_client):
        """Test date string detection."""
        with patch('subprocess.run') as mock_subprocess:
            mock_subprocess.return_value = Mock(stdout="mdbtools version 0.9.3", stderr="", returncode=0)
            extractor = MDBExtractor(mock_config)
        
        assert extractor._is_date_string('2023-01-01')
        assert extractor._is_date_string('01/15/2023')
        assert extractor._is_date_string('12-25-2023')
        assert not extractor._is_date_string('not a date')
        assert not extractor._is_date_string('2023')
    
    def test_clean_column_names(self, mock_config, mock_storage_client):
        """Test column name cleaning."""
        with patch('subprocess.run') as mock_subprocess:
            mock_subprocess.return_value = Mock(stdout="mdbtools version 0.9.3", stderr="", returncode=0)
            extractor = MDBExtractor(mock_config)
        
        df = pd.DataFrame({
            'Normal Column': [1, 2, 3],
            'Column-With-Dashes': [4, 5, 6],
            'Column@With#Special$Chars': [7, 8, 9],
            'Column   With   Spaces': [10, 11, 12]
        })
        
        cleaned_df = extractor._clean_column_names(df)
        
        assert list(cleaned_df.columns) == [
            'Normal_Column',
            'Column_With_Dashes',
            'Column_With_Special_Chars',
            'Column_With_Spaces'
        ]
    
    @patch('subprocess.run')
    @patch('pyarrow.parquet.write_table')
    def test_extract_table_success(self, mock_write_table, mock_subprocess, mock_config, 
                                  mock_storage_client, tmp_path, sample_csv_data):
        """Test successful table extraction."""
        # Update config to use tmp_path
        mock_config.config.paths.temp_dir = str(tmp_path)
        
        # Create test MDB file and output directory
        mdb_file = tmp_path / "test.mdb"
        mdb_file.touch()
        output_dir = tmp_path / "extraction"
        output_dir.mkdir(parents=True)
        
        # Create expected output file to avoid FileNotFoundError
        output_file = output_dir / "TestTable.parquet"
        output_file.touch()
        
        # Mock subprocess calls
        mock_subprocess.side_effect = [
            Mock(stdout="mdbtools version 0.9.3", stderr="", returncode=0),  # mdb-ver
            Mock(stdout=sample_csv_data, stderr="", returncode=0)  # mdb-export
        ]
        
        extractor = MDBExtractor(mock_config)
        metadata = extractor.extract_table(mdb_file, "TestTable", output_path=output_dir)
        
        # Verify metadata
        assert metadata.table_name == "TestTable"
        assert metadata.row_count == 3
        assert metadata.column_count == 4
        assert len(metadata.columns) == 4
        assert metadata.extraction_duration_seconds > 0
        
        # Verify mdb-export was called correctly
        export_call = mock_subprocess.call_args_list[1]
        assert export_call[0][0] == ['mdb-export', str(mdb_file), 'TestTable']
        
        # Verify parquet was written
        mock_write_table.assert_called_once()
    
    @patch('subprocess.run')
    def test_extract_table_empty(self, mock_subprocess, mock_config, mock_storage_client, tmp_path):
        """Test extracting empty table."""
        # Create test MDB file
        mdb_file = tmp_path / "test.mdb"
        mdb_file.touch()
        
        # Mock subprocess calls
        mock_subprocess.side_effect = [
            Mock(stdout="mdbtools version 0.9.3", stderr="", returncode=0),  # mdb-ver
            Mock(stdout="", stderr="", returncode=0)  # mdb-export returns empty
        ]
        
        extractor = MDBExtractor(mock_config)
        
        with pytest.raises(ValueError, match="appears to be empty"):
            extractor.extract_table(mdb_file, "EmptyTable")
    
    @patch('subprocess.run')
    def test_extract_table_export_error(self, mock_subprocess, mock_config, mock_storage_client, tmp_path):
        """Test handling mdb-export error."""
        # Create test MDB file
        mdb_file = tmp_path / "test.mdb"
        mdb_file.touch()
        
        # Mock subprocess calls
        mock_subprocess.side_effect = [
            Mock(stdout="mdbtools version 0.9.3", stderr="", returncode=0),  # mdb-ver
            subprocess.CalledProcessError(1, 'mdb-export', stderr="Table not found")
        ]
        
        extractor = MDBExtractor(mock_config)
        
        with pytest.raises(RuntimeError, match="Failed to extract table"):
            extractor.extract_table(mdb_file, "NonExistentTable")
    
    @patch.object(MDBExtractor, 'list_tables')
    @patch.object(MDBExtractor, 'extract_table')
    def test_extract_all_tables_success(self, mock_extract_table, mock_list_tables, 
                                       mock_config, mock_storage_client, tmp_path):
        """Test extracting all tables."""
        # Create test MDB file
        mdb_file = tmp_path / "test.mdb"
        mdb_file.touch()
        
        # Mock table list
        mock_list_tables.return_value = ["Table1", "Table2", "Table3"]
        
        # Mock successful extractions
        mock_extract_table.side_effect = [
            ExtractionMetadata(
                source_file=str(mdb_file),
                table_name="Table1",
                extraction_timestamp=datetime.utcnow(),
                row_count=10,
                column_count=5,
                columns=[],
                parquet_size_bytes=1000,
                gcs_path="",
                extraction_duration_seconds=1.0
            ),
            ExtractionMetadata(
                source_file=str(mdb_file),
                table_name="Table2",
                extraction_timestamp=datetime.utcnow(),
                row_count=20,
                column_count=3,
                columns=[],
                parquet_size_bytes=2000,
                gcs_path="",
                extraction_duration_seconds=1.5
            ),
            ExtractionMetadata(
                source_file=str(mdb_file),
                table_name="Table3",
                extraction_timestamp=datetime.utcnow(),
                row_count=30,
                column_count=7,
                columns=[],
                parquet_size_bytes=3000,
                gcs_path="",
                extraction_duration_seconds=2.0
            )
        ]
        
        with patch('subprocess.run') as mock_subprocess:
            mock_subprocess.return_value = Mock(stdout="mdbtools version 0.9.3", stderr="", returncode=0)
            extractor = MDBExtractor(mock_config)
        
        manifest = extractor.extract_all_tables(mdb_file)
        
        # Verify manifest
        assert manifest.total_tables == 3
        assert manifest.extracted_tables == 3
        assert len(manifest.table_metadata) == 3
        assert len(manifest.failed_tables) == 0
        assert len(manifest.skipped_tables) == 0
    
    @patch.object(MDBExtractor, 'list_tables')
    @patch.object(MDBExtractor, 'extract_table')
    def test_extract_all_tables_with_filters(self, mock_extract_table, mock_list_tables,
                                            mock_config, mock_storage_client, tmp_path):
        """Test extracting tables with include/exclude patterns."""
        # Create test MDB file
        mdb_file = tmp_path / "test.mdb"
        mdb_file.touch()
        
        # Mock table list
        mock_list_tables.return_value = ["HD2023", "IC2023", "EF2023", "TestTable", "TempTable"]
        
        # Mock successful extraction
        mock_extract_table.return_value = ExtractionMetadata(
            source_file=str(mdb_file),
            table_name="HD2023",
            extraction_timestamp=datetime.utcnow(),
            row_count=10,
            column_count=5,
            columns=[],
            parquet_size_bytes=1000,
            gcs_path="",
            extraction_duration_seconds=1.0
        )
        
        with patch('subprocess.run') as mock_subprocess:
            mock_subprocess.return_value = Mock(stdout="mdbtools version 0.9.3", stderr="", returncode=0)
            extractor = MDBExtractor(mock_config)
        
        # Include only tables starting with HD, IC, or EF
        # Exclude any table containing "Temp"
        manifest = extractor.extract_all_tables(
            mdb_file,
            include_pattern=r"^(HD|IC|EF)",
            exclude_pattern=r".*Temp.*"
        )
        
        # Verify filtering
        assert manifest.total_tables == 5
        assert manifest.extracted_tables == 3  # HD2023, IC2023, EF2023
        assert len(manifest.skipped_tables) == 2  # TestTable, TempTable
        
        # Verify extract_table was called for the right tables
        assert mock_extract_table.call_count == 3
    
    @patch.object(MDBExtractor, 'list_tables')
    @patch.object(MDBExtractor, 'extract_table')
    def test_extract_all_tables_with_failures(self, mock_extract_table, mock_list_tables,
                                             mock_config, mock_storage_client, tmp_path):
        """Test handling extraction failures."""
        # Create test MDB file
        mdb_file = tmp_path / "test.mdb"
        mdb_file.touch()
        
        # Mock table list
        mock_list_tables.return_value = ["Table1", "Table2", "Table3"]
        
        # Mock mixed results - Table2 fails
        def extract_side_effect(mdb_path, table_name, output_path=None):
            if table_name == "Table2":
                raise Exception("Extraction failed")
            return ExtractionMetadata(
                source_file=str(mdb_path),
                table_name=table_name,
                extraction_timestamp=datetime.utcnow(),
                row_count=10,
                column_count=5,
                columns=[],
                parquet_size_bytes=1000,
                gcs_path="",
                extraction_duration_seconds=1.0
            )
        
        mock_extract_table.side_effect = extract_side_effect
        
        with patch('subprocess.run') as mock_subprocess:
            mock_subprocess.return_value = Mock(stdout="mdbtools version 0.9.3", stderr="", returncode=0)
            extractor = MDBExtractor(mock_config)
        
        manifest = extractor.extract_all_tables(mdb_file)
        
        # Verify manifest
        assert manifest.total_tables == 3
        assert manifest.extracted_tables == 2
        assert len(manifest.failed_tables) == 1
        assert "Table2" in manifest.failed_tables
    
    def test_upload_extraction_to_gcs(self, mock_config, mock_storage_client, tmp_path):
        """Test uploading extraction results to GCS."""
        _, mock_bucket, mock_blob = mock_storage_client
        
        # Create test files
        local_dir = tmp_path / "extracted"
        local_dir.mkdir()
        (local_dir / "Table1.parquet").touch()
        (local_dir / "Table2.parquet").touch()
        
        # Create manifest
        manifest = ExtractionManifest(
            source_file="test.mdb",
            extraction_timestamp=datetime.utcnow(),
            total_tables=2,
            extracted_tables=2,
            skipped_tables=[],
            failed_tables=[],
            table_metadata=[
                ExtractionMetadata(
                    source_file="test.mdb",
                    table_name="Table1",
                    extraction_timestamp=datetime.utcnow(),
                    row_count=10,
                    column_count=5,
                    columns=[],
                    parquet_size_bytes=1000,
                    gcs_path="",
                    extraction_duration_seconds=1.0
                ),
                ExtractionMetadata(
                    source_file="test.mdb",
                    table_name="Table2",
                    extraction_timestamp=datetime.utcnow(),
                    row_count=20,
                    column_count=3,
                    columns=[],
                    parquet_size_bytes=2000,
                    gcs_path="",
                    extraction_duration_seconds=1.5
                )
            ],
            total_duration_seconds=2.5
        )
        
        with patch('subprocess.run') as mock_subprocess:
            mock_subprocess.return_value = Mock(stdout="mdbtools version 0.9.3", stderr="", returncode=0)
            extractor = MDBExtractor(mock_config)
        
        result = extractor.upload_extraction_to_gcs(local_dir, 2023, manifest)
        
        # Verify result
        assert result["status"] == "success"
        assert len(result["uploaded_files"]) == 2
        assert result["manifest_path"] == "gs://test-bucket/extracted/2023/metadata/extraction_manifest.json"
        
        # Verify blob calls
        assert mock_bucket.blob.call_count >= 3  # 2 parquet files + 1 manifest
        
        # Verify GCS paths were updated in metadata
        assert manifest.table_metadata[0].gcs_path == "gs://test-bucket/extracted/2023/tables/Table1.parquet"
        assert manifest.table_metadata[1].gcs_path == "gs://test-bucket/extracted/2023/tables/Table2.parquet"
