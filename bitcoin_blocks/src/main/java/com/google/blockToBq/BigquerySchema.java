package com.google.blockToBq;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Field.Mode;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;

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
        Field.newBuilder("timestamp", LegacySQLTypeName.INTEGER).build(),
        Field.newBuilder("difficultyTarget", LegacySQLTypeName.INTEGER)
            .setDescription("Returns the difficulty of the proof of work that this block "
                + "should meet encoded in compact form. The BlockChain verifies that this "
                + "is not too easy by looking at the length of the chain when the block is "
                + "added. To find the actual value the hash should be compared against, use "
                + "getDifficultyTargetAsInteger(). Note that this is not the same as the "
                + "difficulty value reported by the Bitcoin \"getdifficulty\" RPC that you "
                + "may see on various block explorers. That number is the result of applying a "
                + "formula to the underlying difficulty to normalize the minimum to 1. Calculating "
                + "the difficulty that way is currently unsupported.").build(),
        Field.newBuilder("nonce", LegacySQLTypeName.INTEGER).build(),
        Field.newBuilder("version", LegacySQLTypeName.INTEGER).build(),
        Field.newBuilder("height", LegacySQLTypeName.INTEGER).build(),
        Field.newBuilder("work_terahash", LegacySQLTypeName.INTEGER)
            .setMode(Mode.NULLABLE).build(),
        Field.newBuilder("work_error", LegacySQLTypeName.STRING)
            .setMode(Mode.NULLABLE).build(),
        Field.newBuilder("transactions", LegacySQLTypeName.RECORD, transaction)
            .setMode(Mode.REPEATED).build()
    );
  }
}
