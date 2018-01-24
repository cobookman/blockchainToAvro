package com.google.blockToBq;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Field.Mode;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;

public class BigquerySchema {

  public static Schema schema() {
    FieldList outputs = FieldList.of(
        Field.newBuilder("output_satoshis", LegacySQLTypeName.INTEGER)
            .setMode(Mode.NULLABLE).build(),
        Field.newBuilder("output_script_bytes", LegacySQLTypeName.BYTES).build(),
        Field.newBuilder("output_script_string", LegacySQLTypeName.STRING)
            .setMode(Mode.NULLABLE).build(),
        Field.newBuilder("output_script_string_error", LegacySQLTypeName.STRING)
            .setMode(Mode.NULLABLE).build(),
        Field.newBuilder("output_pubkey_base58", LegacySQLTypeName.STRING)
            .setMode(Mode.NULLABLE).build(),
        Field.newBuilder("output_pubkey_base58_error", LegacySQLTypeName.STRING)
            .setMode(Mode.NULLABLE).build()
    );

    FieldList inputs = FieldList.of(
        Field.newBuilder("input_script_bytes", LegacySQLTypeName.BYTES).build(),
        Field.newBuilder("input_script_string", LegacySQLTypeName.STRING)
            .setMode(Mode.NULLABLE).build(),
        Field.newBuilder("input_script_string_error", LegacySQLTypeName.STRING)
            .setMode(Mode.NULLABLE).build(),
        Field.newBuilder("input_sequence_number", LegacySQLTypeName.INTEGER).build(),
        Field.newBuilder("input_pubkey_base58", LegacySQLTypeName.STRING)
            .setMode(Mode.NULLABLE).build(),
        Field.newBuilder("input_pubkey_base58_error", LegacySQLTypeName.STRING)
            .setMode(Mode.NULLABLE).build()
    );

    FieldList transaction = FieldList.of(
        Field.newBuilder("transaction_id", LegacySQLTypeName.STRING).build(),
        Field.newBuilder("inputs", LegacySQLTypeName.RECORD, inputs)
            .setMode(Mode.REPEATED).build(),
        Field.newBuilder("outputs", LegacySQLTypeName.RECORD, outputs)
            .setMode(Mode.REPEATED).build()
    );

    return Schema.of(
        Field.newBuilder("block_id", LegacySQLTypeName.STRING).build(),
        Field.newBuilder("previous_block", LegacySQLTypeName.STRING).build(),
        Field.newBuilder("merkle_root", LegacySQLTypeName.STRING).build(),
        Field.newBuilder("timestamp", LegacySQLTypeName.TIMESTAMP).build(),
        Field.newBuilder("difficulty", LegacySQLTypeName.INTEGER).build(),
        Field.newBuilder("nonce", LegacySQLTypeName.INTEGER).build(),
        Field.newBuilder("version", LegacySQLTypeName.INTEGER).build(),
        Field.newBuilder("height", LegacySQLTypeName.INTEGER).build(),
        Field.newBuilder("work", LegacySQLTypeName.INTEGER)
            .setMode(Mode.NULLABLE).build(),
        Field.newBuilder("work_error", LegacySQLTypeName.STRING)
            .setMode(Mode.NULLABLE).build(),
        Field.newBuilder("transactions", LegacySQLTypeName.RECORD, transaction)
            .setMode(Mode.REPEATED).build()
    );
  }
}
