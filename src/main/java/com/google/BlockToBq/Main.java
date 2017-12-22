package com.google.BlockToBq;

import com.google.BlockToBq.DownloadChain.BlockListener;
import com.google.BlockToBq.Ingestion.AvroFileWriter;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.bitcoinj.core.Block;
import org.bitcoinj.store.BlockStoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
  private static AvroFileWriter writer;
  private static ThreadedBqIngestion threadedBqIngestion;
  private static DownloadChain downloadChain;
  private static final Logger log = LoggerFactory.getLogger(Main.class);

  public static void main(String[] args)
      throws IOException, ExecutionException, InterruptedException, BlockStoreException {


    File file = new File(args[0]);
    writer = new AvroFileWriter(file);
    Ingestion ingestion = new Ingestion(writer);
    threadedBqIngestion = new ThreadedBqIngestion(ingestion);

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        shutdown();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }));

    while (true) {
      try {
        downloadChain = new DownloadChain();
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

  private static void shutdown() throws IOException, InterruptedException {
    System.out.println("shutting down");
    if (downloadChain != null) {
      System.out.println("download of blockchain:\tstopping");
      downloadChain.stop();
      System.out.println("download of blockchain:\tstopped");
    }
    if (threadedBqIngestion != null) {
      System.out.println("ingestion:\tstopping");
      threadedBqIngestion.shutdown();
      System.out.println("ingestion:\tstopped");
    }
    if (writer != null) {
      System.out.println("writer:\tstopping");
      writer.close();
      System.out.println("writer:\tstopped");
    }
    System.out.println("done shutting down");
  }

  public static class ThreadedBqIngestion implements BlockListener {
    private ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 4);
    private Ingestion ingestion;

    public void shutdown() throws InterruptedException {
      executor.shutdown();
      executor.awaitTermination(1000, TimeUnit.MILLISECONDS);
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
