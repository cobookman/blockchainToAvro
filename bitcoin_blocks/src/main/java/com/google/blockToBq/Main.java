package com.google.blockToBq;

import com.google.blockToBq.AvroWriter.Callback;
import com.google.blockToBq.ThreadHelpers.ThreadPool;
import com.google.cloud.WriteChannel;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.storage.Acl;
import com.google.cloud.storage.Acl.Role;
import com.google.cloud.storage.Acl.User;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.io.ByteStreams;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
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
  private static AvroWriter writer;
  private static BitcoinBlockHandler bitcoinBlockHandler;
  private static BitcoinBlockDownloader bitcoinBlockDownloader;
  private static OnFileRotation onFileRotation;
  private static final Logger log = LoggerFactory.getLogger(Main.class);

  public static void main(String[] args)
      throws InterruptedException, IOException, BlockStoreException {
    Options options = new Options();

    Option bucket = new Option("b", "bucket", true,
        "gcs bucket where to save blocks");
    bucket.setRequired(true);
    options.addOption(bucket);

    Option workdir = new Option("w", "workdir", true,
        "directory to save blocks in filesystem");
    workdir.setRequired(true);
    options.addOption(workdir);


    Option threads = new Option("t", "threads", true,
        "number of workers to use for processing blocks");
    threads.setRequired(false);
    options.addOption(threads);

    Option frt = new Option("r", "rotationtime", true,
        "how often to rotate file (in seconds)");
    frt.setRequired(true);
    options.addOption(frt);


    Option db = new Option("d", "dblocation", true,
        "where to store the block database");
    db.setRequired(true);
    options.addOption(db);

    Option dataset = new Option("bd", "dataset", true,
        "dataset for bigquery");
    dataset.setRequired(true);
    options.addOption(dataset);

    Option table = new Option("bt", "table", true,
        "table for bigquery");
    table.setRequired(true);
    options.addOption(table);

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
    setup(cmd);
  }

  /** Resync to latest bitcoin block. */
  private static void setup(CommandLine cmd)
      throws IOException, InterruptedException, BlockStoreException {
    NetworkParameters networkParameters = new MainNetParams();

    log.info("startup: Instantializing Storage Driver");
    Storage storage = StorageOptions.getDefaultInstance().getService();

    log.info("startup: Instantializing BigQuery Driver");
    BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

    log.info("startup: Instantializing Bigquery Dataset");
    Dataset dataset = bigquery.getDataset(cmd.getOptionValue("dataset"));
    if (dataset == null || !dataset.exists()) {
      dataset = bigquery.create(DatasetInfo.of(cmd.getOptionValue("dataset")));
    }

    log.info("startup: Instantializing Bigquery Table");
    Table table = dataset.get(cmd.getOptionValue("table"));
    if (table == null || !table.exists()) {
      table = dataset.create(
          cmd.getOptionValue("table"),
          StandardTableDefinition.of(BigquerySchema.schema()));
    }

    log.info("startup: Instantializing Writer");
    onFileRotation = new OnFileRotation(
        storage,
        cmd.getOptionValue("bucket"),
        table);
    writer = new AvroWriter(
        cmd.getOptionValue("workdir"),
        Integer.parseInt(cmd.getOptionValue("rotationtime")),
        onFileRotation);

    log.info("startup: Instantializing Bitcoin downloader");
    int numWorkers = Integer.parseInt(cmd.getOptionValue("threads"));
    bitcoinBlockHandler = new BitcoinBlockHandler(writer, numWorkers);
    bitcoinBlockDownloader = new BitcoinBlockDownloader(cmd.getOptionValue("dblocation"));

    log.info("startup: Starting Blockchain Download");
    bitcoinBlockDownloader.start(networkParameters, bitcoinBlockHandler);

    // block forever
    while (true) {
      Thread.sleep(5000);
      System.out.println("OnFileRotation QueueSize: " + onFileRotation.getQueueSize()
          + "\tBitcoinBlockHandler QueueSize: " + bitcoinBlockHandler.getQueueSize());
      System.out.flush();
    }
  }

  public static class OnFileRotation implements Callback {
    private Storage storage;
    private String bucket;
    private Table table;
    private ThreadPool executor;

    public int getQueueSize() {
      return this.executor.getQueue().size();
    }
    public OnFileRotation(Storage storage, String bucket, Table table) {
      this.storage = storage;
      this.bucket = bucket;
      this.table = table;
      this.executor = new ThreadPool(1, 2, 0L, TimeUnit.MILLISECONDS);
    }

    public void stop() {
      this.executor.stop();
    }

    @Override
    public void callback(String filepath) {
      log.info("OnFileRotation.callback submitting work executor");
      executor.execute(() -> doWork(filepath));
    }

    private void doWork(String filepath) {
      Exception exception = null;
      for (int i = 0; i < 5; i++) {
        try {
          doWorkTry(filepath);
          return;
        } catch (Exception e) {
          log.error("Error doing work on : " + filepath + " " + e.getMessage());
          exception = e;
        }
      }

      throw new RuntimeException(exception);
    }

    private void doWorkTry(String filepath) {
      String filename = Paths.get(filepath).getFileName().toString();
      log.info("OnFileRotation.callback: " + filename + " Starting callback for file");
      // upload to GCS
      BlobInfo blobInfo = BlobInfo
          .newBuilder(bucket, filename)
          .setAcl(new ArrayList<>(Arrays.asList(Acl.of(User.ofAllUsers(), Role.READER))))
          .build();

      // Upload to GCS
      log.info("OnFileRotation.callback: " + filename + " uploading to gcs");
      try (FileInputStream is = new FileInputStream(new File(filepath))) {
        ReadableByteChannel readChannel = Channels.newChannel(is);
        WriteChannel writeChannel = storage.writer(blobInfo);
        ByteStreams.copy(readChannel, writeChannel);
        writeChannel.close();
        readChannel.close();

        log.info("OnFileRotation.callback: " + filename + " uploaded to gcs successfully");

      } catch (IOException e) {
        log.error("OnFileRotation.callback: " + filename + "upload to gcs " + e);
        return;
      }

      // Ingest to BQ
      String gcsPath = String.format("gs://%s/%s", bucket, filename);
      log.info("OnFileRotation.callback: " + filename + " ingestion to bq of " + gcsPath);
      Job loadJob = table.load(FormatOptions.avro(), gcsPath);
      try {
        loadJob.waitFor();
        if (loadJob.getStatus().getError() != null) {
          log.error("OnFileRotation.callback: " + filename + " error data to bq: " + loadJob.getStatus().getError());
          throw new RuntimeException(loadJob.getStatus().getError().getMessage());
        } else {
          log.info("OnFileRotation.callback: " + filename + " uploaded to bq successfully " + gcsPath);
        }

      } catch (InterruptedException e) {
        log.error("OnFileRotation.callback: " + filename + " bq ingest gcs error (InterruptedException): " + e + " " + gcsPath);
        return;

      } catch (RuntimeException e) {
        log.error("OnFileRotation.callback: " + filename + " bq ingest gcs (RuntimeException): " + e + " " + gcsPath);
        return;
      }

      // Delete File as now done
      try {
        Files.delete(Paths.get(filepath));
        log.info("OnFileRotation.callback: " + filename + " cleaned up successfully ");

      } catch (IOException e) {
        log.error("OnFileRotation.callback: " + filename + " cleanup file " + e);
        return;
      }
    }
  }


  private static void attachShutdownListener() {
    Runtime.getRuntime().addShutdownHook(new Thread() {
      public void run() {
        try {
          Main.teardown();
        } catch (InterruptedException e) {
          e.printStackTrace();
        } catch (IOException e) {
          e.printStackTrace();
        }
        System.err.println("STOPPED");
        System.err.flush();

        // flush any logs
        try {
          Thread.sleep(50);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    });
  }

  private static void teardown() throws InterruptedException, IOException {
    System.err.println("teardown() bitcoinBlockDownloader - stopping");
    if (bitcoinBlockDownloader != null) {
      bitcoinBlockDownloader.stop();
    }
    System.err.println("teardown() bitcoinBlockDownloader - stopped");

    System.err.println("teardown() bitcoinBlockHandler - stopping");
    if (bitcoinBlockHandler != null) {
      bitcoinBlockHandler.stop();
    }
    System.err.println("teardown() bitcoinBlockHandler - stopped");

    System.err.println("teardown() writer - stopping");
    if (writer != null) {
      writer.close();
    }
    System.err.println("teardown() writer - stopped");

    System.err.println("teardown() onFileRotation - stopping");
    if (onFileRotation != null) {
      onFileRotation.stop();
    }
    System.err.println("teardown() onFileRotation - stopped");
  }
}
