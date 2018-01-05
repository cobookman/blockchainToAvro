package com.google.blockToBq;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import org.bitcoinj.core.NetworkParameters;
import org.bitcoinj.params.MainNetParams;
import org.bitcoinj.store.BlockStoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
  private static AvroWriter writer;
  private static BlockHandler blockHandler;
  private static Downloader downloader;
  private static final Logger log = LoggerFactory.getLogger(Main.class);

  public static void main(String[] args)
      throws IOException, ExecutionException, InterruptedException, BlockStoreException {
    attachShutdownListener();
    NetworkParameters networkParameters = new MainNetParams();

    File file = new File(args[0]);
    writer = new AvroWriter(file);
    blockHandler = new BlockHandler(writer);
    downloader = new Downloader();
    downloader.start(networkParameters, blockHandler);

    while (true) {
      if (downloader.isDone()) {
        System.out.println("Done Downloading");
        shutdown();
      } else {
        Thread.sleep(1000);
      }
    }
  }

  private static void attachShutdownListener() {
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        shutdown();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }));
  }

  private static void shutdown() throws IOException, InterruptedException {
    System.out.println("shutting down");
    if (downloader != null) {
      System.out.println("download of blockchain:\tstopping");
      downloader.stop();
      System.out.println("download of blockchain:\tstopped");
    }

    if (blockHandler != null) {
      System.out.println("block queue:\tfinishing");
      blockHandler.stop();
      System.out.println("block queue:\tfinished");
    }

    if (writer != null) {
      System.out.println("writer:\tstopping");
      writer.close();
      System.out.println("writer:\tstopped");
    }

    System.out.println("done shutting down");
  }
}
