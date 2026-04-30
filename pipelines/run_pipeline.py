import os
from google.cloud import storage, bigquery
from pipelines.config import (
    PROJECT_ID,
    BUCKET_NAME,
    RAW_PREFIX,
    PROCESSED_PREFIX,
    RAW_DATASET,
    STG_DATASET,
    FACT_DATASET,
)
from pipelines.big_queries import get_stg_query, get_fact_query

storage_client = storage.Client(project=PROJECT_ID)
bq_client = bigquery.Client(project=PROJECT_ID)


def list_raw_csv_files():
    blobs = storage_client.list_blobs(BUCKET_NAME, prefix=RAW_PREFIX)
    return [blob.name for blob in blobs if blob.name.endswith(".csv")]


def get_country_name(blob_name: str) -> str:
    filename = os.path.basename(blob_name).lower()
    return filename.split("_")[0]


def get_table_names(country: str):
    raw_table = f"{PROJECT_ID}.{RAW_DATASET}.{country}_raw_sales"
    stg_table = f"{PROJECT_ID}.{STG_DATASET}.{country}_stg_sales"
    fact_table = f"{PROJECT_ID}.{FACT_DATASET}.{country}_fact_sales"
    return raw_table, stg_table, fact_table


def load_csv_to_bigquery(blob_name, target_table):
    uri = f"gs://{BUCKET_NAME}/{blob_name}"
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    load_job = bq_client.load_table_from_uri(uri, target_table, job_config=job_config)
    load_job.result()
    print(f"Loaded {blob_name} into {target_table}")


def run_query(query, label):
    job = bq_client.query(query)
    job.result()
    print(f"{label} completed")


def move_to_processed(blob_name):
    bucket = storage_client.bucket(BUCKET_NAME)
    source_blob = bucket.blob(blob_name)
    destination_name = blob_name.replace(RAW_PREFIX, PROCESSED_PREFIX, 1)
    bucket.copy_blob(source_blob, bucket, destination_name)
    source_blob.delete()
    print(f"Moved {blob_name} to {destination_name}")


def main():
    raw_files = list_raw_csv_files()

    if not raw_files:
        print("No new files found in raw/")
        return

    for blob_name in raw_files:
        country = get_country_name(blob_name)
        raw_table, stg_table, fact_table = get_table_names(country)

        print(f"Processing {blob_name} for country: {country}")
        load_csv_to_bigquery(blob_name, raw_table)

        stg_query = get_stg_query(raw_table, stg_table)
        fact_query = get_fact_query(stg_table, fact_table)

        run_query(stg_query, f"{country}_stg_sales build")
        run_query(fact_query, f"{country}_fact_sales build")

        move_to_processed(blob_name)

    print("Pipeline run complete")


if __name__ == "__main__":
    main()