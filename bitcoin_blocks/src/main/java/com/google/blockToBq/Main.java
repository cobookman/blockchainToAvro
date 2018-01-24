package com.google.blockToBq;

import com.google.blockToBq.AvroWriter.Callback;
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
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
    sync(cmd);
  }

  /** Resync to latest bitcoin block. */
  private static void sync(CommandLine cmd)
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
    OnFileRotation onFileRotation = new OnFileRotation(
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
    while (!bitcoinBlockDownloader.isDone()) {
      Thread.sleep(1000);
    }

    log.info("startup: Done downloading. Pausing for a bit to cleanup and finish any remaining work");
    onFileRotation.blockTillDone();
    Thread.sleep(10000);
    System.exit(0);
  }

  public static class OnFileRotation implements Callback {
    private Storage storage;
    private String bucket;
    private Table table;
    private ExecutorService executor;

    public OnFileRotation(Storage storage, String bucket, Table table) {
      this.storage = storage;
      this.bucket = bucket;
      this.table = table;
      this.executor = Executors.newFixedThreadPool(8);
    }

    @Override
    public void callback(String filepath) {
      executor.execute(() -> doWork(filepath));
    }

    public void blockTillDone() throws InterruptedException {
      executor.shutdown();
      executor.awaitTermination(60, TimeUnit.SECONDS);
    }

    private void doWork(String filepath) {
      log.info("Starting callback");

      String filename = Paths.get(filepath).getFileName().toString();
      // upload to GCS
      BlobInfo blobInfo = BlobInfo
          .newBuilder(bucket, filename)
          .setAcl(new ArrayList<>(Arrays.asList(Acl.of(User.ofAllUsers(), Role.READER))))
          .build();

      // Upload to GCS
      try (FileInputStream is = new FileInputStream(new File(filepath))) {
        storage.create(blobInfo, is);
        log.info("OnFileRotation.callback: uploaded to gcs successfully " + filepath);

      } catch (IOException e) {
        log.error("OnFileRotation.callback: upload to gcs " + e + filepath);
        return;
      }

      // Ingest to BQ
      String gcsPath = String.format("gs://%s/%s", bucket, filename);
      log.info("OnFileRotation.callback: ingestion to bq of " + gcsPath);
      Job loadJob = table.load(FormatOptions.avro(), gcsPath);
      try {
        loadJob.waitFor();
        if (loadJob.getStatus().getError() != null) {
          throw new RuntimeException(loadJob.getStatus().getError().getMessage());
        } else {
          log.info("OnFileRotation.callback: uploaded to bq successfully " + filepath);
        }

      } catch (InterruptedException e) {
        log.error("OnFileRotation.callback: bq ingest gcs " + e + filepath);
        return;

      } catch (RuntimeException e) {
        log.error("OnFileRotation.callback: bq ingest gcs " + e + filepath);
        return;
      }

      // Delete File as now done
      try {
        Files.delete(Paths.get(filepath));
        log.info("OnFileRotation.callback: cleaned up successfully " + filepath);

      } catch (IOException e) {
        log.error("OnFileRotation.callback: cleanup file " + e + filepath);
        return;
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
