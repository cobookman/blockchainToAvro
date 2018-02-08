#!/bin/bash
DESTINATION="bigquery-public-data:bitcoin_blockchain.transactions"
BILLING_PROJECT="bitcoin-bigquery"
# Dedup duplicate blocks
QUERY=""
QUERY="WITH dedup AS( "
QUERY+=" SELECT "
QUERY+="   * "
QUERY+=" FROM ( "
QUERY+="   SELECT "
QUERY+="     *, "
QUERY+="     ROW_NUMBER() OVER (PARTITION by block_id) row_number "
QUERY+="   FROM "
QUERY+="     \`bitcoin-bigquery.bitcoin.blocks\` "
QUERY+=" ) "
QUERY+=" WHERE "
QUERY+="   row_number = 1) "

# Unpack transactions as unique row
QUERY+="SELECT "
QUERY+="  timestamp, "
QUERY+="  transactions.*, "
QUERY+="  block_id, "
QUERY+="  previous_block, "
QUERY+="  merkle_root, "
QUERY+="  nonce, "
QUERY+="  version, "
QUERY+="  work_terahash, "
QUERY+="  work_error "
QUERY+="FROM "
QUERY+="  dedup "
QUERY+="JOIN "
QUERY+="  UNNEST(transactions) AS transactions "

bq query \
  --project="${BILLING_PROJECT}" \
  --destination_table="${DESTINATION}" \
  --use_legacy_sql=false \
  --replace=true \
  "${QUERY}"
