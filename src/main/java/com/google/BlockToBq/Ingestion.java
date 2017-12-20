package com.google.BlockToBq;

import com.google.BlockToBq.Generated.BitcoinBlock;
import com.google.BlockToBq.Generated.BitcoinInput;
import com.google.BlockToBq.Generated.BitcoinOutput;
import com.google.BlockToBq.Generated.BitcoinTransaction;
import com.google.BlockToBq.Generated.BitcoinTransaction.Builder;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.bitcoinj.core.Block;
import org.bitcoinj.core.ScriptException;
import org.bitcoinj.core.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Ingestion {
  private static final Logger log = LoggerFactory.getLogger(Ingestion.class);
  private AvroFileWriter writer;

  public Ingestion(AvroFileWriter avroFileWriter) {
    // this.bucket = bucket;
    // this.storage = StorageOptions.getDefaultInstance().getService();
    writer = avroFileWriter;

  }

  public static class AvroFileWriter {
    DataFileWriter<BitcoinTransaction> dataFileWriter;

    public AvroFileWriter(File file) throws IOException {
      DatumWriter<BitcoinTransaction> blockWriter = new SpecificDatumWriter<>(BitcoinTransaction.class);
      dataFileWriter = new DataFileWriter<>(blockWriter);
      dataFileWriter.create(BitcoinTransaction.getClassSchema(), file);
    }

    public synchronized void write(List<BitcoinTransaction> transactions) throws IOException {
      for (BitcoinTransaction transaction : transactions) {
        dataFileWriter.append(transaction);
      }
    }

    public void close() throws IOException {
      dataFileWriter.close();
    }
  }

  // /** Gets the write channel for storing the block in gs://bucket/blockId. */
  // public WriteChannel getWriteChannel(Block block) {
  //   BlobInfo blobInfo = BlobInfo.newBuilder(this.bucket, block.getHashAsString() + ".avro").build();
  //   return this.storage.writer(blobInfo);
  // }

  public List<BitcoinTransaction> blockToAvro(Block block) {
    BitcoinBlock.Builder bitcoinBlockBuilder = BitcoinBlock.newBuilder()
        .setBlockId(block.getHashAsString())
        .setPreviousBlock(block.getPrevBlockHash().toString())
        .setMerkleRoot(block.getMerkleRoot().toString())
        .setTimestamp(block.getTime().getTime())
        .setDifficulty(block.getDifficultyTarget())
        .setNonce(block.getNonce())
        .setVersion(block.getVersion());

    try {
      bitcoinBlockBuilder.setWork(block.getWork().longValueExact());
    } catch(ArithmeticException e) {
      bitcoinBlockBuilder.setWork(-1L);
    }

    final BitcoinBlock bitcoinBlock = bitcoinBlockBuilder.build();

    List<Transaction> transactions = block.getTransactions();
    if (transactions == null) {
      transactions = new ArrayList<>();
    }

    List<BitcoinTransaction> out = transactions.stream().map(transaction -> {
      Builder rowBuilder = BitcoinTransaction.newBuilder();
      rowBuilder.setBlock(bitcoinBlock);
      rowBuilder.setTransactionId(transaction.getHashAsString());

      rowBuilder.setInputs(transaction.getInputs().stream().map(tIn -> {
        BitcoinInput.Builder inputBuilder = BitcoinInput.newBuilder();
        inputBuilder.setScriptBytes(ByteBuffer.wrap(tIn.getScriptBytes()));
        try {
          inputBuilder.setScriptString(tIn.getScriptSig().toString());
        } catch (ScriptException e) {
          inputBuilder.setScriptString("");
        }

        inputBuilder.setSequenceNumber(tIn.getSequenceNumber());
        return inputBuilder.build();
      }).collect(Collectors.toList()));

      rowBuilder.setOutputs(transaction.getOutputs().stream().map(tOut -> {
        BitcoinOutput.Builder output = BitcoinOutput.newBuilder();
        if (tOut.getValue() != null) {
          output.setSatoshis(tOut.getValue().getValue());
        } else {
          output.setSatoshis(-1L);
        }
        output.setScriptBytes(ByteBuffer.wrap(tOut.getScriptBytes()));
        try {
          output.setScriptString(tOut.getScriptPubKey().toString());
        } catch (ScriptException e) {
          output.setScriptString("");
        }
        return output.build();
      }).collect(Collectors.toList()));
      return rowBuilder.build();
    }).collect(Collectors.toList());
    return out;
  }

  public void onBlock(Block block) throws IOException, StorageException {
    // // AVRO writers to memory
    // DatumWriter<BitcoinTransaction> blockWriter = new SpecificDatumWriter<>(BitcoinTransaction.class);
    // DataFileWriter<BitcoinTransaction> dataFileWriter = new DataFileWriter<>(blockWriter);
    // ByteArrayOutputStream bout = new ByteArrayOutputStream();
    // dataFileWriter.create(BitcoinTransaction.getClassSchema(), bout);

    List<BitcoinTransaction> transactions = blockToAvro(block);
    writer.write(transactions);

    // dataFileWriter.close();
    // writer.write(ByteBuffer.wrap(bout.toByteArray()));
    // writer.close();
  }
}
