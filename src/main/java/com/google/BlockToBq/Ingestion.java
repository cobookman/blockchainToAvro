package com.google.BlockToBq;

import com.google.BlockToBq.Generated.BitcoinBlock;
import com.google.BlockToBq.Generated.BitcoinInput;
import com.google.BlockToBq.Generated.BitcoinOutput;
import com.google.BlockToBq.Generated.BitcoinTransaction;
import com.google.BlockToBq.Generated.BitcoinTransaction.Builder;
import com.google.cloud.storage.StorageException;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificDatumWriter;
import org.bitcoinj.core.*;
import org.bitcoinj.params.MainNetParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Ingestion {
  private static final Logger log = LoggerFactory.getLogger(Ingestion.class);
  private AvroFileWriter writer;

  public Ingestion(AvroFileWriter avroFileWriter) {
    writer = avroFileWriter;

  }

  public static class AvroFileWriter {
    DataFileWriter<BitcoinTransaction> dataFileWriter;

    public AvroFileWriter(File file) throws IOException {
      DatumWriter<BitcoinTransaction> blockWriter = new SpecificDatumWriter<>(BitcoinTransaction.class);
      dataFileWriter = new DataFileWriter<>(blockWriter);
      if (file.exists()) {
        dataFileWriter.appendTo(file);
      } else {
        dataFileWriter.create(BitcoinTransaction.getClassSchema(), file);
      }
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

  private List<BitcoinTransaction> blockToAvro(Block block) {
    NetworkParameters params = new MainNetParams();

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
          inputBuilder.setScriptString(""); //TODO null instead?
	  inputBuilder.setEtlNotes("Exception:ScriptException:1");
        }

        if (tIn.isCoinBase()) {
          inputBuilder.setPubkey("");
        } else {
          try {
            inputBuilder.setPubkey(tIn.getFromAddress().toBase58());
          } catch (ScriptException e) {
            inputBuilder.setPubkey(""); //TODO null instead?
            inputBuilder.setEtlNotes("Exception:ScriptException:2");
          }
        }

        inputBuilder.setSequenceNumber(tIn.getSequenceNumber());
        return inputBuilder.build();
      }).collect(Collectors.toList()));

      rowBuilder.setOutputs(transaction.getOutputs().stream().map(tOut -> {
        BitcoinOutput.Builder output = BitcoinOutput.newBuilder();
        if (tOut.getValue() != null) {
          output.setSatoshis(tOut.getValue().getValue());
        } else {
          output.setSatoshis(-1L); //TODO set to null, update schema
          output.setEtlNotes("Exception:null value for satoshis");
        }
        output.setScriptBytes(ByteBuffer.wrap(tOut.getScriptBytes()));
        try {
          output.setScriptString(tOut.getScriptPubKey().toString());
        } catch (ScriptException e) {
          output.setScriptString(""); //TODO null instead?
          output.setEtlNotes("Exception:ScriptException:3");
        }

        try {
          output.setPubkey(tOut.getScriptPubKey().getToAddress(params).toBase58());
        } catch (ScriptException e) {
          output.setPubkey(""); //TODO null instead?
          output.setEtlNotes("Exception:ScriptException:4");
        }

        return output.build();
      }).collect(Collectors.toList()));
      return rowBuilder.build();
    }).collect(Collectors.toList());
    
    return out;
  }

  public void onBlock(Block block) throws IOException, StorageException {
    List<BitcoinTransaction> transactions = blockToAvro(block);
    writer.write(transactions);
    Schema schema = transactions.get(0).getSchema();
    final DatumWriter<Object> jsonWriter = new GenericDatumWriter<Object>(schema);
    final JsonEncoder jsonEncoder = EncoderFactory.get().jsonEncoder(schema, System.out);
    //for (BitcoinTransaction t : transactions) {
    //  jsonWriter.write(t, jsonEncoder);
    //}
    //jsonEncoder.flush();
    //System.out.println();
  }
}
