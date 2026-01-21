import logging
from pathlib import Path
from google.cloud import storage
from google.cloud import bigquery

logger = logging.getLogger(__name__)

BUCKET_NAME = "recipe-etl-bucket"
DATASET = "recipe_dw"
TABLE_NAME = "recipes"

PARQUET_PATH = Path("data/processed/recipes.parquet")


def upload_to_gcs():
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)

    blob = bucket.blob("recipes/recipes.parquet")
    blob.upload_from_filename(PARQUET_PATH)

    logger.info("Uploaded parquet to gs://%s/recipes/recipes.parquet", BUCKET_NAME)


def load_into_bigquery():
    bq = bigquery.Client()

    table_id = f"{bq.project}.{DATASET}.{TABLE_NAME}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition="WRITE_TRUNCATE",  # full reload
    )

    uri = f"gs://{BUCKET_NAME}/recipes/recipes.parquet"

    load_job = bq.load_table_from_uri(uri, table_id, job_config=job_config)
    load_job.result()

    logger.info("Loaded data into BigQuery table %s", table_id)


def run_bigquery_load():
    logger.info("Starting BigQuery load...")
    upload_to_gcs()
    load_into_bigquery()
    logger.info("BigQuery load complete.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_bigquery_load()
