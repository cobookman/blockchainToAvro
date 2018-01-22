#!/bin/bash
# Exit on any errors
set -e

# Globals
ROOT_API="https://api.bitcoincharts.com/v1/csv"

BQ_DATASET="bitcoin"
BQ_PROJECT="bitcoin-bigquery"
BQ_SCHEMA="timestamp:timestamp,priceUSD:float,volumeBTC:float"

EXCHANGES=(
  "coinbaseUSD"
  "krakenUSD"
  "bitfinexUSD"
  "hitbtcUSD"
  "bitstampUSD")

# Functions
function ingest() {
  local exchange=$1
  local dl_path=$(mktemp "/tmp/ingest_prices.XXXXXX.${exchange}.csv.gz")
  local blue="\033[1;4;34m"
  local lightblue="\033[2;4;34m"
  local nc="\033[0m"
  echo -e "${blue}Ingestion for ${exchange}${nc}"

  echo -e "${lightblue}Downloading ${dl_path}${nc}"
  wget "$ROOT_API/$exchange.csv.gz" -O $dl_path

  #echo -e "${lightblue}Extracting csv${nc}"
  #gzip --stdout -d -k $dl_path > "$SAVE_DIR/$exchange.csv"

  echo -e "${lightblue}Uploading ${dl_path} to BQ${nc}"
  bq --project_id=$BQ_PROJECT load "${BQ_DATASET}.${exchange}" $dl_path "$BQ_SCHEMA"

  echo -e "${lightblue}Cleaning up${nc}"
  rm $dl_path
}


function main() {
  #if [ -z $SAVE_DIR ]; then
  #  echo "Please specify a save dir as first argument"
  #  exit 1
  #fi

  echo -e "\n\n"
  echo -e "===================="
  echo -e "Starting Ingestion"
  echo -e "===================="

  for exchange in "${EXCHANGES[@]}"; do
    ingest $exchange
  done

  echo -e "\n\n"
  echo -e "========================"
  echo -e "Done With Ingestion"
  echo -e "======================="

}

# Do not put anything below this
main
