package com.google.blockToBq;

import com.google.blockToBq.generated.AvroBitcoinBlock;
import com.google.blockToBq.generated.AvroBitcoinInput;
import com.google.blockToBq.generated.AvroBitcoinOutput;
import com.google.blockToBq.generated.AvroBitcoinTransaction;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.bitcoinj.core.Block;
import org.bitcoinj.core.ScriptException;
import org.bitcoinj.core.Transaction;
import org.bitcoinj.core.TransactionInput;
import org.bitcoinj.core.TransactionOutput;
import org.bitcoinj.params.MainNetParams;

public class BitcoinBlockHandler implements BitcoinBlockDownloader.BlockListener {
  public final int WRITE_RETRIES = 3;
  public final int NUM_WORKERS = Runtime.getRuntime().availableProcessors() * 4;
  private AvroWriter writer;
  private ExecutorService executor;

  public BitcoinBlockHandler(AvroWriter writer) {
    this.writer = writer;
    this.executor = Executors.newFixedThreadPool(NUM_WORKERS);
  }

  /** Called whenver a new block is downloaded. */
  public void onBlock(long blockHeight, Block block) {
    executor.execute(() -> processBlock(blockHeight, block));
  }

  /** Handles the processing of new blocks. */
  private void processBlock(long blockHeight, Block block) {
    AvroBitcoinBlock avroBlock = convertBlockToAvro(blockHeight, block);

    IOException ioException = null;
    for (int i = 0; i < WRITE_RETRIES; ++i) {
      try {
        writer.write(avroBlock);
      } catch (IOException e) {
        ioException = e;
      }
    }
    if (ioException != null) {
      ioException.printStackTrace();
    }
  }

  /** Stops accepting new blocks & flushes queue. **/
  public void stop() throws InterruptedException {
    executor.shutdown();
    executor.awaitTermination(10, TimeUnit.SECONDS);
  }

  /** Converts a Bitcoinj Block to Avro representation. */
  public static AvroBitcoinBlock convertBlockToAvro(long blockHeight, Block block) {
    AvroBitcoinBlock.Builder blockBuilder = AvroBitcoinBlock.newBuilder()
        .setBlockId(block.getHashAsString())
        .setPreviousBlock(block.getPrevBlockHash().toString())
        .setMerkleRoot(block.getMerkleRoot().toString())
        .setTimestamp(block.getTime().getTime())
        .setDifficulty(block.getDifficultyTarget())
        .setNonce(block.getNonce())
        .setVersion(block.getVersion())
        .setHeight(blockHeight);

    try {
      blockBuilder.setWork(block.getWork().longValueExact());
    } catch (ArithmeticException e) {
      blockBuilder.setWork(null);
      blockBuilder.setWorkError(e.getMessage());
    }

    if (block.getTransactions() == null) {
      blockBuilder.setTransactions(new ArrayList<>());
    } else {
      blockBuilder.setTransactions(block.getTransactions().stream()
          .map(BitcoinBlockHandler::convertTransctionToAvro)
          .collect(Collectors.toList()));
    }

    return blockBuilder.build();
  }

  public static AvroBitcoinTransaction convertTransctionToAvro(Transaction transaction) {
    AvroBitcoinTransaction.Builder transactionBuilder = AvroBitcoinTransaction.newBuilder();

    transactionBuilder.setTransactionId(transaction.getHashAsString());

    transactionBuilder.setInputs(transaction.getInputs().stream()
        .map(BitcoinBlockHandler::convertInputToAvro)
        .collect(Collectors.toList()));

    transactionBuilder.setOutputs(transaction.getOutputs().stream()
        .map(BitcoinBlockHandler::convertOutputToAvro)
        .collect(Collectors.toList()));

    return transactionBuilder.build();
  }

  public static AvroBitcoinInput convertInputToAvro(TransactionInput input) {
    AvroBitcoinInput.Builder builder = AvroBitcoinInput.newBuilder()
        .setScriptBytes(ByteBuffer.wrap(input.getScriptBytes()))
        .setSequenceNumber(input.getSequenceNumber());

    // Parse script string
    try {
      builder.setScriptString(input.getScriptSig().toString());
    } catch (ScriptException e) {
      builder.setScriptString(null);
      builder.setScriptStringError(e.getMessage());
    }

    // Parse input address
    if (input.isCoinBase()) {
      builder.setPubkeyBase58("");
    } else {

      try {
        builder.setPubkeyBase58(input.getFromAddress().toBase58());
      } catch (ScriptException e) {
        builder.setPubkeyBase58(null);
        builder.setPubkeyBase58Error(e.getMessage());
      }
    }

    return builder.build();
  }

  public static AvroBitcoinOutput convertOutputToAvro(TransactionOutput output) {
    AvroBitcoinOutput.Builder builder = AvroBitcoinOutput.newBuilder()
        .setScriptBytes(ByteBuffer.wrap(output.getScriptBytes()));

    // parse out satoshis
    if (output.getValue() != null) {
      builder.setSatoshis(output.getValue().getValue());
    } else {
      builder.setSatoshis(null);
    }

    // parse script string
    try {
      builder.setScriptString(output.getScriptPubKey().toString());
    } catch (ScriptException e) {
      builder.setScriptString(null);
      builder.setScriptStringError(e.getMessage());
    }

    // parse pubkey
    try {
      builder.setPubkeyBase58(output.getScriptPubKey().getToAddress(MainNetParams.get()).toBase58());
    } catch (ScriptException e) {
      builder.setPubkeyBase58(null);
      builder.setPubkeyBase58Error(e.getMessage());
    }

    return builder.build();
  }
}

