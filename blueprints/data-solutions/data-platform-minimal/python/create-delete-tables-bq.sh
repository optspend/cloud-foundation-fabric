declare -a tables=(
"allergies" "careplans" "claims" "claims_transactions"
"conditions" "devices" "encounters" "imaging_studies"
"immunizations" "medications" "observations" "organizations"
"patients" "payer_transitions" "payers" "procedures"
"providers" "supplies"
)

for table in "${tables[@]}"
do
  echo "Deleting table $table..."
  bq rm -f -t keen-button-463715-v2:synthea_dataset.$table
done


for file in "${tables[@]}"
do
  echo "Loading $file.parquet into BigQuery..."
  bq load \
    --source_format=PARQUET \
    --replace \
    --autodetect \
    keen-button-463715-v2:synthea_dataset.$file \
    gs://demo-lnd-cs-0/landing-parquet/$file/$file.parquet
done

