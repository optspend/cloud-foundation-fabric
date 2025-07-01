import os
import time
from google.cloud import storage, dataplex_v1, bigquery
from google.api_core.exceptions import Conflict, NotFound

# --- Configuration ---
PROJECT_ID = os.environ.get("GOOGLE_CLOUD_PROJECT") or "your-gcp-project-id" # Replace with your project ID
REGION = "us-central1"  # Choose a suitable region for Dataplex and BigQuery
GCS_BUCKET_NAME = "your-csv-source-bucket"  # Replace with your GCS bucket name
LAKE_ID = "my-csv-lake"
ZONE_ID = "raw-csv-data"
ASSET_ID = "csv-files-asset"
BIGQUERY_DATASET_ID = "raw_csv_data_bq" # BigQuery dataset where tables will be created

# Local directory containing CSV files
LOCAL_CSV_DIR = "csv_files" # Create this directory and place your CSVs inside

# --- Helper Functions ---

def upload_csv_to_gcs(local_file_path, bucket_name, destination_blob_name):
    """Uploads a file to a GCS bucket."""
    storage_client = storage.Client(project=PROJECT_ID)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    try:
        blob.upload_from_filename(local_file_path)
        print(f"Uploaded {local_file_path} to gs://{bucket_name}/{destination_blob_name}")
    except Exception as e:
        print(f"Error uploading {local_file_path}: {e}")
        raise

def create_dataplex_lake(project_id, region, lake_id, display_name):
    """Creates a Dataplex Lake."""
    client = dataplex_v1.DataplexServiceClient()
    parent = f"projects/{project_id}/locations/{region}"
    lake = dataplex_v1.Lake(display_name=display_name)

    try:
        operation = client.create_lake(parent=parent, lake_id=lake_id, lake=lake)
        print(f"Creating lake {lake_id}...")
        response = operation.result(timeout=300)  # Wait for creation to complete
        print(f"Lake {response.name} created successfully.")
        return response
    except Conflict:
        print(f"Lake {lake_id} already exists.")
        return client.get_lake(name=f"{parent}/lakes/{lake_id}")
    except Exception as e:
        print(f"Error creating lake {lake_id}: {e}")
        raise

def create_dataplex_zone(project_id, region, lake_id, zone_id, display_name):
    """Creates a Dataplex Zone within a Lake."""
    client = dataplex_v1.DataplexServiceClient()
    parent = f"projects/{project_id}/locations/{region}/lakes/{lake_id}"
    zone = dataplex_v1.Zone(
        display_name=display_name,
        type_=dataplex_v1.Zone.Type.RAW,  # Or CURATED, depending on your data stage
        resource_spec=dataplex_v1.Zone.ResourceSpec(
            location_type=dataplex_v1.Zone.ResourceSpec.LocationType.MULTI_REGION if 'us' in region or 'eu' in region else dataplex_v1.Zone.ResourceSpec.LocationType.SINGLE_REGION,
        ),
        discovery_spec=dataplex_v1.Zone.DiscoverySpec(
            enabled=True,
            csv_options=dataplex_v1.Zone.DiscoverySpec.CsvOptions(
                header_rows=1,  # Assuming your CSVs have a header row
                delimiter=",",
                disable_type_inference=False # Set to True if you want all columns as strings
            ),
            # Add other options like `exclude_patterns` or `include_patterns` if needed
            # For BigQuery publishing to work, ensure a dataset is defined for publishing
            # BigQuery publishing is configured at the Asset level, not Zone.
        )
    )

    try:
        operation = client.create_zone(parent=parent, zone_id=zone_id, zone=zone)
        print(f"Creating zone {zone_id}...")
        response = operation.result(timeout=300)
        print(f"Zone {response.name} created successfully.")
        return response
    except Conflict:
        print(f"Zone {zone_id} already exists.")
        return client.get_zone(name=f"{parent}/zones/{zone_id}")
    except Exception as e:
        print(f"Error creating zone {zone_id}: {e}")
        raise

def create_dataplex_asset(project_id, region, lake_id, zone_id, asset_id, bucket_name, bq_dataset_id):
    """Creates a Dataplex Asset for a GCS bucket and enables discovery with BigQuery publishing."""
    client = dataplex_v1.DataplexServiceClient()
    parent = f"projects/{project_id}/locations/{region}/lakes/{lake_id}/zones/{zone_id}"
    
    # Construct resource name for GCS bucket
    gcs_resource_name = f"projects/{project_id}/buckets/{bucket_name}"

    asset = dataplex_v1.Asset(
        display_name=asset_id,
        resource_spec=dataplex_v1.Asset.ResourceSpec(
            type_=dataplex_v1.Asset.ResourceSpec.Type.STORAGE_BUCKET,
            name=gcs_resource_name,
        ),
        discovery_spec=dataplex_v1.Asset.DiscoverySpec(
            enabled=True,
            # Inherit CSV options from zone or override if needed
            csv_options=dataplex_v1.Asset.DiscoverySpec.CsvOptions(
                header_rows=1,
                delimiter=",",
                disable_type_inference=False
            ),
            # This is crucial for BigQuery table creation
            # Note: Dataplex Universal Catalog automatically creates external tables
            # in BigQuery based on discovery settings when the asset is configured.
            # No explicit `bigquery_publishing_config` is directly set here in Python client,
            # it's implicitly handled by Dataplex when discovery is enabled for BigQuery-compatible formats.
            # If you want to explicitly control the BigQuery dataset for publishing, you might need to
            # set the BigQuery project/dataset directly on the asset's resource spec during creation
            # or rely on Dataplex's default behavior for the zone/lake.
            # For Cloud Storage assets, Dataplex will publish entities to Data Catalog and
            # potentially create external BigQuery tables if enabled and configured properly at the asset level.
        )
    )

    # For explicit BigQuery external table creation for *each* CSV,
    # it's often more reliable to do it directly via the BigQuery API
    # if Dataplex's auto-publishing doesn't meet specific needs or isn't immediate.
    # However, for general discovery and cataloging, the asset config is sufficient.

    try:
        operation = client.create_asset(parent=parent, asset_id=asset_id, asset=asset)
        print(f"Creating asset {asset_id}...")
        response = operation.result(timeout=300)
        print(f"Asset {response.name} created successfully.")
        return response
    except Conflict:
        print(f"Asset {asset_id} already exists.")
        return client.get_asset(name=f"{parent}/assets/{asset_id}")
    except Exception as e:
        print(f"Error creating asset {asset_id}: {e}")
        raise

def create_bigquery_dataset(project_id, dataset_id, region):
    """Creates a BigQuery dataset."""
    bq_client = bigquery.Client(project=project_id)
    dataset_ref = bq_client.dataset(dataset_id)
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = region

    try:
        dataset = bq_client.create_dataset(dataset)
        print(f"BigQuery dataset {dataset.project}.{dataset.dataset_id} created.")
        return dataset
    except Conflict:
        print(f"BigQuery dataset {dataset.project}.{dataset.dataset_id} already exists.")
        return bq_client.get_dataset(dataset_ref)
    except Exception as e:
        print(f"Error creating BigQuery dataset {dataset_id}: {e}")
        raise

def create_bigquery_external_table_from_csv(
    project_id, dataset_id, table_id, gcs_uri, skip_leading_rows=1
):
    """Creates an external BigQuery table pointing to a GCS CSV file."""
    bq_client = bigquery.Client(project=project_id)
    dataset_ref = bq_client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=skip_leading_rows,
        autodetect=True,  # BigQuery will infer schema from CSV
        max_bad_records=0 # Adjust as needed for error tolerance
    )

    try:
        # Check if table already exists
        bq_client.get_table(table_ref)
        print(f"BigQuery table {table_id} already exists. Skipping creation.")
        return
    except NotFound:
        print(f"Creating BigQuery external table {table_id} from {gcs_uri}...")
        load_job = bq_client.load_table_from_uri(
            gcs_uri, table_ref, job_config=job_config
        )
        load_job.result()  # Waits for the job to complete
        print(f"BigQuery external table {table_id} created successfully.")
        return bq_client.get_table(table_ref)
    except Exception as e:
        print(f"Error creating BigQuery external table {table_id}: {e}")
        raise

def main():
    print("--- Starting Dataplex and BigQuery Automation for CSVs ---")

    # 1. Create GCS Bucket if it doesn't exist (optional, assume it exists for simplicity)
    # storage_client = storage.Client(project=PROJECT_ID)
    # try:
    #     bucket = storage_client.create_bucket(GCS_BUCKET_NAME, location=REGION)
    #     print(f"Bucket {GCS_BUCKET_NAME} created.")
    # except Conflict:
    #     print(f"Bucket {GCS_BUCKET_NAME} already exists.")
    # except Exception as e:
    #     print(f"Error creating bucket {GCS_BUCKET_NAME}: {e}")
    #     return

    # 2. Upload CSV files to GCS
    if not os.path.exists(LOCAL_CSV_DIR):
        print(f"Error: Local CSV directory '{LOCAL_CSV_DIR}' not found. Please create it and place CSV files inside.")
        return

    csv_files = [f for f in os.listdir(LOCAL_CSV_DIR) if f.endswith(".csv")]
    if not csv_files:
        print(f"No CSV files found in '{LOCAL_CSV_DIR}'. Please add some CSV files.")
        return

    gcs_file_uris = []
    for csv_file in csv_files:
        local_path = os.path.join(LOCAL_CSV_DIR, csv_file)
        destination_blob_name = f"csv_data/{csv_file}" # Organize CSVs in a subfolder in GCS
        upload_csv_to_gcs(local_path, GCS_BUCKET_NAME, destination_blob_name)
        gcs_file_uris.append(f"gs://{GCS_BUCKET_NAME}/{destination_blob_name}")

    # 3. Create Dataplex Lake
    lake = create_dataplex_lake(PROJECT_ID, REGION, LAKE_ID, "My CSV Data Lake")

    # 4. Create Dataplex Zone
    zone = create_dataplex_zone(PROJECT_ID, REGION, LAKE_ID, ZONE_ID, "Raw CSV Zone")

    # 5. Create Dataplex Asset for the GCS bucket
    asset = create_dataplex_asset(PROJECT_ID, REGION, LAKE_ID, ZONE_ID, ASSET_ID, GCS_BUCKET_NAME, BIGQUERY_DATASET_ID)

    print("\n--- Dataplex Discovery Process Started ---")
    print("Dataplex will now discover files in gs://{} and create entities in its Catalog.".format(GCS_BUCKET_NAME))
    print("This may take a few minutes for discovery to complete and BigQuery external tables to appear (if auto-published).")
    print("You can monitor discovery status in the Dataplex UI under the asset's 'Entities' tab.")
    # Give Dataplex some time to perform initial discovery
    time.sleep(60) # Wait for a minute for discovery to kick in

    # 6. Create BigQuery Dataset
    bq_dataset = create_bigquery_dataset(PROJECT_ID, BIGQUERY_DATASET_ID, REGION)

    # 7. Create BigQuery External Tables directly (as a fallback/alternative to Dataplex auto-publishing)
    # Dataplex *should* auto-publish BigQuery external tables for discovered CSVs if configured properly.
    # However, explicitly creating them via BQ API gives more control and immediate confirmation.
    print("\n--- Creating BigQuery External Tables (direct method) ---")
    for uri in gcs_file_uris:
        # Extract table name from the GCS URI (e.g., 'my_file.csv' -> 'my_file')
        table_name = os.path.basename(uri).replace(".csv", "").replace("-", "_")
        create_bigquery_external_table_from_csv(PROJECT_ID, BIGQUERY_DATASET_ID, table_name, uri)

    print("\n--- Automation Complete ---")
    print(f"Dataplex Lake: {LAKE_ID}, Zone: {ZONE_ID}, Asset: {ASSET_ID} configured.")
    print(f"CSV files from '{LOCAL_CSV_DIR}' uploaded to gs://{GCS_BUCKET_NAME}/csv_data/.")
    print(f"BigQuery Dataset: {BIGQUERY_DATASET_ID}.")
    print("External BigQuery tables created (or will be auto-discovered by Dataplex) for your CSVs.")
    print("You can now view and query these tables in the BigQuery console and explore metadata in Dataplex Catalog.")

if __name__ == "__main__":
    # Ensure you have 'csv_files' directory with some CSVs in the same directory as the script.
    # Example 'csv_files/data1.csv':
    # id,name,value
    # 1,Alice,100
    # 2,Bob,200

    # Example 'csv_files/data2.csv':
    # product,price,quantity
    # Laptop,1200.50,5
    # Mouse,25.00,20

    main()
