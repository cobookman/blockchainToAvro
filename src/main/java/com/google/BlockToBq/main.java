package com.google.BlockToBq;

import com.google.BlockToBq.DownloadChain.BlockListener;
import com.google.BlockToBq.Ingestion.AvroFileWriter;
import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.bitcoinj.core.Block;
import org.bitcoinj.store.BlockStoreException;

public class main {
  public static void main(String[] args)
      throws IOException, ExecutionException, InterruptedException, BlockStoreException {
    File file = new File(args[0]);
    AvroFileWriter writer = new AvroFileWriter(file);
    final Ingestion ingestion = new Ingestion(writer);

    ThreadedBqIngestion threadedBqIngestion = new ThreadedBqIngestion(ingestion);
    DownloadChain downloadChain = new DownloadChain();
    downloadChain.start(threadedBqIngestion);
    downloadChain.blockTillDone();
    System.out.println("Done");
    writer.close();
  }

  public static class ThreadedBqIngestion implements BlockListener {
    private ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 4);
    private Ingestion ingestion;

    ThreadedBqIngestion(Ingestion ingestion) {
      this.ingestion = ingestion;
    }

    @Override
    public void onBlock(Block block) {
      executor.execute(() -> {
        Exception error = null;
        for (int i = 0; i < 5; i++) {
          try {
            ingestion.onBlock(block);
            return;
          } catch (Exception e) {
            error = e;
            try {
              Thread.sleep(100);
            } catch (InterruptedException e1) {
              e1.printStackTrace();
            }
          }
        }
        error.printStackTrace();
      });
    }
  }
}
