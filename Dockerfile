# Windsurf IPEDS Pipeline - Container Image
# Base: Python 3.12 slim + mdbtools for MDB file extraction

FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Install system dependencies
# mdbtools: for extracting Microsoft Access Database files
# build-essential: for compiling Python packages with C extensions
RUN apt-get update && apt-get install -y \
    mdbtools \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy dependency files first for better layer caching
COPY pyproject.toml poetry.lock* ./

# Install Python dependencies
# If poetry.lock exists, use it for reproducible builds
# Otherwise, install from pyproject.toml
RUN pip install --no-cache-dir poetry==1.7.1 && \
    poetry config virtualenvs.create false && \
    poetry install --no-interaction --no-ansi --no-root --only main || \
    pip install typer python-dotenv google-cloud-bigquery google-cloud-storage pandas pyarrow requests pydantic

# Copy application code
COPY src/ ./src/
COPY config/ ./config/

# Create required directories
RUN mkdir -p /tmp/windsurf-ipeds /secrets

# Set Python path
ENV PYTHONPATH=/app

# Default command - show help
CMD ["python", "-m", "src.cli", "--help"]

# Labels for container metadata
LABEL maintainer="Blue Sky Mind LLC" \
      description="IPEDS ingestion pipeline for BigQuery" \
      version="0.1.0"
