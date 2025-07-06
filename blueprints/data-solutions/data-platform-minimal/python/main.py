import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os
from google.cloud import storage

storage_client = storage.Client()

bucket_name = "demo-lnd-cs-0"
source_prefix = "synthea/"
target_prefix = "landing-parquet/"

csv_files = [
    "allergies.csv", "careplans.csv", "claims.csv", "claims_transactions.csv",
    "conditions.csv", "devices.csv", "encounters.csv", "imaging_studies.csv",
    "immunizations.csv", "medications.csv", "observations.csv", "organizations.csv",
    "patients.csv", "payer_transitions.csv", "payers.csv", "procedures.csv",
    "providers.csv", "supplies.csv"
]

bucket = storage_client.bucket(bucket_name)

for file in csv_files:
    # Download CSV to temp
    blob = bucket.blob(source_prefix + file)
    local_csv = f"/tmp/{file}"
    blob.download_to_filename(local_csv)
    
    # Convert to Parquet
    df = pd.read_csv(local_csv)
    table = pa.Table.from_pandas(df)
    local_parquet = local_csv.replace(".csv", ".parquet")
    pq.write_table(table, local_parquet)

    # Upload to GCS landing-parquet/
    parquet_blob = bucket.blob(target_prefix + file.replace(".csv", ".parquet"))
    parquet_blob.upload_from_filename(local_parquet)

    print(f"Uploaded {file} as Parquet.")

