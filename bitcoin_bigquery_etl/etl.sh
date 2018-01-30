#!/bin/bash
DESTINATION="bigquery-public-data:bitcoin_blockchain.transactions"
BILLING_PROJECT="bitcoin-bigquery"

QUERY=""
QUERY+="SELECT "
QUERY+="  timestamp, "
QUERY+="  transactions.*, "
QUERY+="  block_id, "
QUERY+="  previous_block, "
QUERY+="  merkle_root, "
QUERY+="  difficultyTarget, "
QUERY+="  nonce, "
QUERY+="  version "
QUERY+="FROM "
QUERY+="  \`bitcoin-bigquery.bitcoin.blocks_raw\` "
QUERY+="JOIN "
QUERY+="  UNNEST(transactions) AS transactions "

bq query \
  --project="${BILLING_PROJECT}" \
  --destination_table="${DESTINATION}" \
  --use_legacy_sql=false \
  --replace=true \
  "${QUERY}"
