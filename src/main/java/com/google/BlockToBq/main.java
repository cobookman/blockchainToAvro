package com.google.BlockToBq;

import com.google.BlockToBq.DownloadChain.BlockListener;
import com.google.BlockToBq.Ingestion.AvroFileWriter;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.bitcoinj.core.Block;
import org.bitcoinj.store.BlockStoreException;

public class main {
  private static AvroFileWriter writer;
  private static ThreadedBqIngestion threadedBqIngestion;

  public static void main(String[] args)
      throws IOException, ExecutionException, InterruptedException, BlockStoreException {
    File file = new File(args[0]);
    writer = new AvroFileWriter(file);
    Ingestion ingestion = new Ingestion(writer);
    threadedBqIngestion = new ThreadedBqIngestion(ingestion);

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        shutdown();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }));

    while (true) {
      try {
        DownloadChain downloadChain = new DownloadChain();
        downloadChain.start(threadedBqIngestion);
        downloadChain.blockTillDone();
        System.out.println("Done");
        writer.close();
        return;
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  private static void shutdown() throws IOException {
    threadedBqIngestion.shutdown();
    writer.close();
  }

  public static class ThreadedBqIngestion implements BlockListener {
    private ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 4);
    private Ingestion ingestion;

    public void shutdown() {
      executor.shutdown();
    }
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
