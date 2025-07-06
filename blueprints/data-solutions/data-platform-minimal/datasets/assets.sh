#!/bin/bash

PROJECT_ID="keen-button-463715-v2"
LOCATION="europe-central2"
LAKE="healthcare-lake"
ZONE="landing-zone"
BUCKET="demo-lnd-cs-0"
PREFIX="synthea"

CSV_FILES=(
  allergies.csv
  careplans.csv
  claims.csv
  claims_transactions.csv
  conditions.csv
  devices.csv
  encounters.csv
  imaging_studies.csv
  immunizations.csv
  medications.csv
  observations.csv
  organizations.csv
  patients.csv
  payer_transitions.csv
  payers.csv
  procedures.csv
  providers.csv
  supplies.csv
)

for FILE in "${CSV_FILES[@]}"; do
  ASSET_NAME="${FILE%.csv}_asset"
  echo "Creating asset: $ASSET_NAME"

  gcloud dataplex assets create "$ASSET_NAME" \
    --project="$PROJECT_ID" \
    --location="$LOCATION" \
    --lake="$LAKE" \
    --zone="$ZONE" \
    --asset-type=STORAGE_BUCKET \
    --resource-name="projects/$PROJECT_ID/locations/$LOCATION/buckets/$BUCKET/objects/$PREFIX/$FILE" \
    --discovery-enabled \
    --discovery-schedule="0 * * * *" \
    --csv-options=delimiter="," \
    --csv-options=header-rows=1
done

