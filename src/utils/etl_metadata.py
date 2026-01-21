# src/utils/etl_metadata.py
"""
ETL Metadata Tracking

Tracks ETL run metadata including timestamps, record counts, and data freshness.
This helps with monitoring and debugging ETL pipelines.
"""
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional
import json

from sqlalchemy import text

from src.utils.db import engine

logger = logging.getLogger(__name__)


def create_etl_metadata_table():
    """Create the etl_runs table if it doesn't exist"""
    create_table = """
    CREATE TABLE IF NOT EXISTS etl_runs (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        run_id VARCHAR(255) NOT NULL UNIQUE,
        run_timestamp DATETIME NOT NULL,
        extract_timestamp DATETIME,
        transform_timestamp DATETIME,
        load_timestamp DATETIME,
        raw_file_path VARCHAR(512),
        parquet_file_path VARCHAR(512),
        records_extracted INT,
        records_transformed INT,
        records_loaded INT,
        status VARCHAR(50) NOT NULL,
        error_message TEXT,
        quality_check_passed BOOLEAN,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        INDEX idx_run_timestamp (run_timestamp),
        INDEX idx_status (status)
    ) CHARACTER SET utf8mb4;
    """
    
    with engine.begin() as conn:
        conn.execute(text(create_table))
    logger.info("Ensured etl_runs table exists")


def log_etl_run(
    run_id: str,
    status: str = "running",
    raw_file_path: Optional[str] = None,
    parquet_file_path: Optional[str] = None,
    records_extracted: Optional[int] = None,
    records_transformed: Optional[int] = None,
    records_loaded: Optional[int] = None,
    quality_check_passed: Optional[bool] = None,
    error_message: Optional[str] = None,
    extract_timestamp: Optional[datetime] = None,
    transform_timestamp: Optional[datetime] = None,
    load_timestamp: Optional[datetime] = None,
):
    """
    Log ETL run metadata to the database.
    
    Args:
        run_id: Unique identifier for this ETL run
        status: Status of the run (running, completed, failed)
        raw_file_path: Path to raw data file
        parquet_file_path: Path to processed parquet file
        records_extracted: Number of records extracted
        records_transformed: Number of records transformed
        records_loaded: Number of records loaded
        quality_check_passed: Whether data quality checks passed
        error_message: Error message if run failed
        extract_timestamp: Timestamp when extract completed
        transform_timestamp: Timestamp when transform completed
        load_timestamp: Timestamp when load completed
    """
    create_etl_metadata_table()
    
    run_timestamp = datetime.utcnow()
    
    # Check if run_id already exists (for updates)
    with engine.begin() as conn:
        check_existing = text("SELECT id FROM etl_runs WHERE run_id = :run_id")
        existing = conn.execute(check_existing, {"run_id": run_id}).fetchone()
        
        if existing:
            # Update existing run
            update_sql = """
            UPDATE etl_runs SET
                status = :status,
                raw_file_path = COALESCE(:raw_file_path, raw_file_path),
                parquet_file_path = COALESCE(:parquet_file_path, parquet_file_path),
                records_extracted = COALESCE(:records_extracted, records_extracted),
                records_transformed = COALESCE(:records_transformed, records_transformed),
                records_loaded = COALESCE(:records_loaded, records_loaded),
                quality_check_passed = COALESCE(:quality_check_passed, quality_check_passed),
                error_message = COALESCE(:error_message, error_message),
                extract_timestamp = COALESCE(:extract_timestamp, extract_timestamp),
                transform_timestamp = COALESCE(:transform_timestamp, transform_timestamp),
                load_timestamp = COALESCE(:load_timestamp, load_timestamp)
            WHERE run_id = :run_id
            """
            conn.execute(
                text(update_sql),
                {
                    "run_id": run_id,
                    "status": status,
                    "raw_file_path": raw_file_path,
                    "parquet_file_path": parquet_file_path,
                    "records_extracted": records_extracted,
                    "records_transformed": records_transformed,
                    "records_loaded": records_loaded,
                    "quality_check_passed": quality_check_passed,
                    "error_message": error_message,
                    "extract_timestamp": extract_timestamp,
                    "transform_timestamp": transform_timestamp,
                    "load_timestamp": load_timestamp,
                }
            )
        else:
            # Insert new run
            insert_sql = """
            INSERT INTO etl_runs (
                run_id, run_timestamp, status,
                raw_file_path, parquet_file_path,
                records_extracted, records_transformed, records_loaded,
                quality_check_passed, error_message,
                extract_timestamp, transform_timestamp, load_timestamp
            ) VALUES (
                :run_id, :run_timestamp, :status,
                :raw_file_path, :parquet_file_path,
                :records_extracted, :records_transformed, :records_loaded,
                :quality_check_passed, :error_message,
                :extract_timestamp, :transform_timestamp, :load_timestamp
            )
            """
            conn.execute(
                text(insert_sql),
                {
                    "run_id": run_id,
                    "run_timestamp": run_timestamp,
                    "status": status,
                    "raw_file_path": raw_file_path,
                    "parquet_file_path": parquet_file_path,
                    "records_extracted": records_extracted,
                    "records_transformed": records_transformed,
                    "records_loaded": records_loaded,
                    "quality_check_passed": quality_check_passed,
                    "error_message": error_message,
                    "extract_timestamp": extract_timestamp,
                    "transform_timestamp": transform_timestamp,
                    "load_timestamp": load_timestamp,
                }
            )
    
    logger.info("Logged ETL run metadata: run_id=%s, status=%s", run_id, status)


def get_latest_run_status() -> Optional[Dict]:
    """Get the status of the most recent ETL run"""
    create_etl_metadata_table()
    
    with engine.begin() as conn:
        query = text("""
            SELECT 
                run_id, run_timestamp, status,
                records_extracted, records_transformed, records_loaded,
                quality_check_passed, error_message
            FROM etl_runs
            ORDER BY run_timestamp DESC
            LIMIT 1
        """)
        result = conn.execute(query).fetchone()
        
        if result:
            return {
                "run_id": result[0],
                "run_timestamp": result[1].isoformat() if result[1] else None,
                "status": result[2],
                "records_extracted": result[3],
                "records_transformed": result[4],
                "records_loaded": result[5],
                "quality_check_passed": result[6],
                "error_message": result[7],
            }
    
    return None


def generate_run_id() -> str:
    """Generate a unique run ID based on timestamp"""
    return datetime.utcnow().strftime("etl_%Y%m%d_%H%M%S_%f")


