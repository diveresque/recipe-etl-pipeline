# src/prefect_flows/recipe_flow.py
import logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)

logger = logging.getLogger(__name__)

from prefect import flow, task

from src.extract.fetch_recipes import fetch_and_save
from src.transform.transform_recipes import process_raw, validate_data_quality
from src.load.load_to_db import create_tables_if_not_exists, load_parquet_to_db
from src.utils.etl_metadata import log_etl_run, generate_run_id, create_etl_metadata_table
import pandas as pd

@task
def t_extract(
    themealdb_categories=None,
    spoonacular_types=None,
    refresh=False,
):
    return fetch_and_save(
        themealdb_categories=themealdb_categories,
        spoonacular_types=spoonacular_types,
        refresh=refresh,
    )

@task
def t_transform(latest_path=None, validate=True):
    return process_raw(latest_path, validate=validate)

@task
def t_validate_quality(parquet_path):
    """Validate data quality after transformation"""
    df = pd.read_parquet(parquet_path)
    checker = validate_data_quality(df, raise_on_failure=False)
    return checker.get_summary()

@task
def t_create_tables():
    create_tables_if_not_exists()

@task
def t_load(path=None):
    load_parquet_to_db(path)

@task
def t_get_record_count(parquet_path):
    """Get record count from parquet file"""
    df = pd.read_parquet(parquet_path)
    return len(df)

@task
def t_get_extract_record_count(raw_path):
    """Get record count from raw JSON file"""
    import json
    with open(raw_path, 'r', encoding='utf8') as f:
        data = json.load(f)
    return len(data) if isinstance(data, list) else 0

@flow
def recipe_etl_flow(
    themealdb_categories=None,
    spoonacular_types=None,
    refresh=False,
    validate_quality=True,
):
    """
    Main ETL flow for recipe data pipeline.
    
    Args:
        themealdb_categories: List of TheMealDB categories to fetch
        spoonacular_types: List of Spoonacular dish types to fetch
        refresh: If True, refresh cached data
        validate_quality: If True, run data quality checks
    """
    run_id = generate_run_id()
    logger.info("Starting ETL run: %s", run_id)
    
    # Create metadata table if needed
    create_etl_metadata_table()
    
    try:
        # Extract
        extract_start = datetime.utcnow()
        raw_path = t_extract(
            themealdb_categories=themealdb_categories,
            spoonacular_types=spoonacular_types,
            refresh=refresh,
        )
        extract_end = datetime.utcnow()
        records_extracted = t_get_extract_record_count(raw_path)
        
        log_etl_run(
            run_id=run_id,
            status="running",
            raw_file_path=str(raw_path),
            records_extracted=records_extracted,
            extract_timestamp=extract_end,
        )
        logger.info("ETL flow: extract task returned raw path %s (%d records)", raw_path, records_extracted)
        
        # Transform
        transform_start = datetime.utcnow()
        parquet_path = t_transform(raw_path, validate=validate_quality)
        transform_end = datetime.utcnow()
        records_transformed = t_get_record_count(parquet_path)
        
        # Quality validation
        quality_passed = True
        if validate_quality:
            quality_summary = t_validate_quality(parquet_path)
            quality_passed = quality_summary['failed'] == 0
            logger.info("Quality checks: %d passed, %d failed", 
                       quality_summary['passed'], quality_summary['failed'])
        
        log_etl_run(
            run_id=run_id,
            status="running",
            parquet_file_path=str(parquet_path),
            records_transformed=records_transformed,
            quality_check_passed=quality_passed,
            transform_timestamp=transform_end,
        )
        logger.info("ETL flow: transform task produced parquet %s (%d records)", parquet_path, records_transformed)
        
        # Load
        load_start = datetime.utcnow()
        t_create_tables()
        load_results = t_load(parquet_path)
        load_end = datetime.utcnow()
        
        # Get actual loaded record count from load function
        records_loaded = load_results.get("recipes_loaded", records_transformed) if load_results else records_transformed
        
        log_etl_run(
            run_id=run_id,
            status="completed",
            records_loaded=records_loaded,
            load_timestamp=load_end,
        )
        
        logger.info("ETL flow completed successfully. Run ID: %s", run_id)
        logger.info("Summary: Extracted=%d, Transformed=%d, Loaded=%d, Quality Passed=%s",
                   records_extracted, records_transformed, records_loaded, quality_passed)
        
    except Exception as e:
        logger.error("ETL flow failed: %s", str(e), exc_info=True)
        log_etl_run(
            run_id=run_id,
            status="failed",
            error_message=str(e),
        )
        raise

if __name__ == "__main__":
    import sys
    
    # Parse command line args for refresh flag
    refresh = "--refresh" in sys.argv or "-r" in sys.argv or any(
        arg in sys.argv for arg in ["--refresh=true", "--refresh=True", "--refresh=1"]
    )
    
    recipe_etl_flow(refresh=refresh)
