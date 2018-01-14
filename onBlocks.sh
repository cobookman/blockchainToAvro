#!/usr/bin/env bash

FILEPATH=$1
BUCKET="gs://some-bucket/blockchain_cunks/"
DEST_TABLE="myproject:mydataset.mytable"

gsutil -o GSUtil:parallel_composite_upload_threshold=150M -m cp $FILEPATH $BUCKET
bq load $DEST_TABLE $BUCKET/$FILEPATH