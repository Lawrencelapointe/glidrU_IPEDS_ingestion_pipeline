# Copyright (c) 2025 Blue Sky Mind LLC
# All Rights Reserved.
# Proprietary and confidential.

"""Main CLI entry point for the GlidrU IPEDS Pipeline."""

import logging
from pathlib import Path
from typing import Optional

import typer
from google.cloud import storage  # type: ignore
from rich.console import Console
from rich.logging import RichHandler

from ..core.config_manager import ConfigManager
from ..downloaders.ipeds_downloader import IPEDSDownloader
from ..extractors.mdb_extractor import MDBExtractor

# Set up rich console for pretty output
console = Console()

# Create Typer app
app = typer.Typer(
    name="glidru-ipeds",
    help="GlidrU IPEDS Pipeline CLI - Download and extract IPEDS data",
    add_completion=False
)


def setup_logging(level: str = "INFO") -> None:
    """Set up logging with rich handler."""
    logging.basicConfig(
        level=level,
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(console=console, rich_tracebacks=True)]
    )


@app.callback()
def main(
    log_level: str = typer.Option(
        "INFO",
        "--log-level",
        "-l",
        help="Logging level",
        case_sensitive=False
    )
) -> None:
    """GlidrU IPEDS Pipeline - Production-grade IPEDS data ingestion."""
    setup_logging(log_level.upper())


@app.command()
def download(
    year: int = typer.Argument(..., help="Academic year to download (e.g., 2023)"),
    version: str = typer.Option(
        "final",
        "--version",
        "-v",
        help="Data version: final, provisional, or revised",
        case_sensitive=False
    ),
    force: bool = typer.Option(
        False,
        "--force",
        "-f",
        help="Force re-download even if file exists in GCS"
    )
) -> None:
    """Download IPEDS data for a specific year."""
    # Validate year
    if year < 2000 or year > 2024:
        console.print("[red]Error: Year must be between 2000 and 2024[/red]")
        raise typer.Exit(1)

    try:
        # Initialize components
        config = ConfigManager()
        downloader = IPEDSDownloader(config)

        # Log start
        console.print(f"[bold blue]Downloading IPEDS data for year {year} (version: {version})[/bold blue]")

        # Perform download
        result = downloader.download_ipeds_data(year, version, force)

        if result["status"] == "exists" and not force:
            console.print(f"[yellow]File already exists in GCS: {result['gcs_path']}[/yellow]")
            console.print("[dim]Use --force to re-download[/dim]")
        elif result["status"] == "success":
            metadata = result["metadata"]
            console.print("[green]✓ Download successful![/green]")
            console.print(f"  File: {metadata['filename']}")
            console.print(f"  Size: {metadata['file_size_bytes']:,} bytes")
            console.print(f"  Duration: {metadata['download_duration_seconds']:.1f} seconds")
            console.print(f"  GCS Path: {metadata['gcs_path']}")

    except Exception as e:
        console.print(f"[red]Error: {str(e)}[/red]")
        logging.exception("Download failed")
        raise typer.Exit(1)


@app.command()
def extract(
    file_path: str = typer.Argument(..., help="Path to MDB file (local or GCS)"),
    table: Optional[str] = typer.Option(
        None,
        "--table",
        "-t",
        help="Specific table to extract (default: all tables)"
    ),
    output_dir: Optional[str] = typer.Option(
        None,
        "--output-dir",
        "-o",
        help="Output directory for Parquet files"
    ),
    include_pattern: Optional[str] = typer.Option(
        None,
        "--include",
        "-i",
        help="Regex pattern for tables to include"
    ),
    exclude_pattern: Optional[str] = typer.Option(
        None,
        "--exclude",
        "-e",
        help="Regex pattern for tables to exclude"
    ),
    upload: bool = typer.Option(
        True,
        "--upload/--no-upload",
        help="Upload results to GCS after extraction"
    )
) -> None:
    """Extract tables from an IPEDS Access database."""
    try:
        # Initialize components
        config = ConfigManager()
        extractor = MDBExtractor(config)

        # Handle GCS paths
        mdb_path = Path(file_path)
        if file_path.startswith("gs://"):
            # Download from GCS to temp location
            console.print("[blue]Downloading MDB file from GCS...[/blue]")
            storage_client = storage.Client()

            # Parse GCS path
            parts = file_path.replace("gs://", "").split("/", 1)
            bucket_name = parts[0]
            blob_path = parts[1] if len(parts) > 1 else ""

            # Download to temp
            temp_dir = Path(config.config.paths.temp_dir) / "downloads"
            temp_dir.mkdir(parents=True, exist_ok=True)
            mdb_path = temp_dir / Path(blob_path).name

            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(blob_path)
            blob.download_to_filename(str(mdb_path))
            console.print(f"[green]✓ Downloaded to {mdb_path}[/green]")

        # Set output directory
        output_path = Path(output_dir) if output_dir else None

        # Extract based on options
        if table:
            # Extract single table
            console.print(f"[blue]Extracting table '{table}'...[/blue]")
            metadata = extractor.extract_table(mdb_path, table, output_path)

            console.print(f"[green]✓ Extracted {table}:[/green]")
            console.print(f"  Rows: {metadata.row_count:,}")
            console.print(f"  Columns: {metadata.column_count}")
            console.print(f"  Size: {metadata.parquet_size_bytes:,} bytes")

        else:
            # Extract all tables
            console.print(f"[blue]Extracting all tables from {mdb_path.name}...[/blue]")
            manifest = extractor.extract_all_tables(
                mdb_path,
                include_pattern=include_pattern,
                exclude_pattern=exclude_pattern,
                output_dir=output_path
            )

            console.print("[green]✓ Extraction complete:[/green]")
            console.print(f"  Total tables: {manifest.total_tables}")
            console.print(f"  Extracted: {manifest.extracted_tables}")
            console.print(f"  Skipped: {len(manifest.skipped_tables)}")
            console.print(f"  Failed: {len(manifest.failed_tables)}")
            console.print(f"  Duration: {manifest.total_duration_seconds:.1f} seconds")

            if manifest.failed_tables:
                console.print("[yellow]Failed tables:[/yellow]")
                for t in manifest.failed_tables:
                    console.print(f"  - {t}")

            # Upload to GCS if requested
            if upload and output_path:
                # Try to determine year from filename
                import re
                year_match = re.search(r'IPEDS(\d{4})', mdb_path.name)
                if year_match:
                    year = int(year_match.group(1))
                    console.print("[blue]Uploading results to GCS...[/blue]")

                    upload_result = extractor.upload_extraction_to_gcs(
                        output_path,
                        year,
                        manifest
                    )

                    console.print(f"[green]✓ Uploaded {len(upload_result['uploaded_files'])} files[/green]")
                    console.print(f"  Manifest: {upload_result['manifest_path']}")
                else:
                    console.print("[yellow]Could not determine year from filename for GCS upload[/yellow]")

    except Exception as e:
        console.print(f"[red]Error: {str(e)}[/red]")
        logging.exception("Extraction failed")
        raise typer.Exit(1)


@app.command()
def list_tables(
    file_path: str = typer.Argument(..., help="Path to MDB file (local or GCS)")
) -> None:
    """List all tables in an IPEDS Access database."""
    try:
        # Initialize components
        config = ConfigManager()
        extractor = MDBExtractor(config)

        # Handle GCS paths
        mdb_path = Path(file_path)
        if file_path.startswith("gs://"):
            # Download from GCS to temp location
            console.print("[blue]Downloading MDB file from GCS...[/blue]")
            storage_client = storage.Client()

            # Parse GCS path
            parts = file_path.replace("gs://", "").split("/", 1)
            bucket_name = parts[0]
            blob_path = parts[1] if len(parts) > 1 else ""

            # Download to temp
            temp_dir = Path(config.config.paths.temp_dir) / "downloads"
            temp_dir.mkdir(parents=True, exist_ok=True)
            mdb_path = temp_dir / Path(blob_path).name

            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(blob_path)
            blob.download_to_filename(str(mdb_path))

        # List tables
        console.print(f"[blue]Listing tables in {mdb_path.name}...[/blue]")
        tables = extractor.list_tables(mdb_path)

        console.print(f"[green]Found {len(tables)} tables:[/green]")
        for table in sorted(tables):
            console.print(f"  • {table}")

    except Exception as e:
        console.print(f"[red]Error: {str(e)}[/red]")
        logging.exception("Failed to list tables")
        raise typer.Exit(1)


@app.command()
def info() -> None:
    """Show pipeline configuration and status."""
    try:
        config = ConfigManager()

        console.print("[bold]GlidrU IPEDS Pipeline Configuration[/bold]")
        console.print(f"  GCS Bucket: {config.config.paths.raw_bucket}")
        console.print(f"  Staging Dataset: {config.config.ipeds.staging_dataset}")
        console.print(f"  Mart Dataset: {config.config.ipeds.mart_dataset}")
        console.print(f"  IPEDS Base URL: {config.config.ipeds.mdb_base_url}")
        console.print(f"  Default Year: {config.config.ipeds.default_year}")

        # Check GCS access
        try:
            storage_client = storage.Client()
            bucket_name = config.config.paths.raw_bucket.replace('gs://', '').rstrip('/')
            bucket = storage_client.bucket(bucket_name)
            list(bucket.list_blobs(max_results=1))
            console.print("[green]✓ GCS access verified[/green]")
        except Exception as e:
            console.print(f"[red]✗ GCS access failed: {str(e)}[/red]")

    except Exception as e:
        console.print(f"[red]Error: {str(e)}[/red]")
        raise typer.Exit(1)


if __name__ == "__main__":
    app()
