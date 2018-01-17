package com.google.blockToBq;

import com.google.cloud.ServiceOptions;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.pubsub.v1.TopicName;
import java.io.IOException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.bitcoinj.core.NetworkParameters;
import org.bitcoinj.params.MainNetParams;
import org.bitcoinj.store.BlockStoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
  private static AvroGcsWriter writer;
  private static BitcoinBlockHandler bitcoinBlockHandler;
  private static BitcoinBlockDownloader bitcoinBlockDownloader;
  private static final Logger log = LoggerFactory.getLogger(Main.class);
  private static final String projectId = ServiceOptions.getDefaultProjectId();

  public static void main(String[] args)
      throws InterruptedException, IOException, BlockStoreException {
    Options options = new Options();

    Option bucket = new Option("b", "bucket", true,
        "gcs bucket where to save blocks");
    bucket.setRequired(true);
    options.addOption(bucket);

    Option topic = new Option("t", "topic", true,
        "pubsub topic for publishing notifications of new block");
    topic.setRequired(true);
    options.addOption(topic);

    Option workers = new Option("w", "workers", true,
        "number of workers to use for processing blocks");
    workers.setRequired(false);
    options.addOption(workers);

    Option db = new Option("d", "dblocation", true,
        "where to store the block database");
    db.setRequired(true);
    options.addOption(db);

    CommandLineParser parser = new DefaultParser();
    HelpFormatter formatter = new HelpFormatter();
    CommandLine cmd;

    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      System.out.println(e.getMessage());
      formatter.printHelp("utility-name", options);
      System.exit(1);
      return;
    }

    attachShutdownListener();
    sync(cmd);
  }

  /** Resync to latest bitcoin block. */
  public static void sync(CommandLine cmd)
      throws IOException, InterruptedException, BlockStoreException {

    NetworkParameters networkParameters = new MainNetParams();
    log.info("instantiating writer");
    TopicName topicName = TopicName.of(projectId, cmd.getOptionValue("topic"));
    Publisher pubsub = Publisher.newBuilder(topicName).build();
    Storage storage = StorageOptions.getDefaultInstance().getService();
    writer = new AvroGcsWriter(pubsub, storage, cmd.getOptionValue("bucket"));

    log.info("instantiating bitcoin handler & downloader");
    int numWorkers = Integer.parseInt(cmd.getOptionValue("workers"));
    bitcoinBlockHandler = new BitcoinBlockHandler(writer, numWorkers);
    bitcoinBlockDownloader = new BitcoinBlockDownloader(cmd.getOptionValue("dblocation"));

    log.info("starting blockchain download");
    bitcoinBlockDownloader.start(networkParameters, bitcoinBlockHandler);

    while (true) {
      if (bitcoinBlockDownloader.isDone()) {
        log.info("Sync'd to head, pausing for 10 seconds");
        Thread.sleep(10000);
        log.info("Restarting sync");
        bitcoinBlockDownloader.start(networkParameters, bitcoinBlockHandler);
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
    log.warn("shutting down");
    if (bitcoinBlockDownloader != null) {
      log.warn("download of blockchain:\tstopping");
      bitcoinBlockDownloader.stop();
      log.warn("download of blockchain:\tstopped");
    }

    if (bitcoinBlockHandler != null) {
      log.warn("block queue:\tfinishing");
      bitcoinBlockHandler.stop();
      log.warn("block queue:\tfinished");
    }

    if (writer != null) {
      log.warn("writer:\tstopping");
      writer.close();
      log.warn("writer:\tstopped");
    }

    log.warn("shutting down: Done");

    // give logs some time to flush (no way to explictly flush :( ).
    Thread.sleep(1000);
  }
}
