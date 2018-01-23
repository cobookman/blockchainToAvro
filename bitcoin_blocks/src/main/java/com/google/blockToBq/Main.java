package com.google.blockToBq;

import com.google.blockToBq.AvroGcsWriter.Callback;
import com.google.cloud.ServiceOptions;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
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


    Option workers = new Option("t", "threads", true,
        "number of workers to use for processing blocks");
    workers.setRequired(false);
    options.addOption(workers);

    Option frt = new Option("r", "rotationtime", true,
        "how often to rotate file (in seconds)");
    frt.setRequired(true);
    options.addOption(frt);


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
  private static void sync(CommandLine cmd)
      throws IOException, InterruptedException, BlockStoreException {
    NetworkParameters networkParameters = new MainNetParams();

    log.info("instantiating writer");
    Storage storage = StorageOptions.getDefaultInstance().getService();
    BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
    Table table = bigquery.getTable(TableId.of("dataset", "table"));
    OnFileRotation onFileRotation = new OnFileRotation(
        storage,
        cmd.getOptionValue("bucket"),
        table);
    writer = new AvroGcsWriter(
        cmd.getOptionValue("workdir"),
        Integer.parseInt(cmd.getOptionValue("rotationtime")),
        onFileRotation);

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

  public static class OnFileRotation implements Callback {
    private Storage storage;
    private String bucket;
    private Table table;

    public OnFileRotation(Storage storage, String bucket, Table table) {
      this.storage = storage;
      this.bucket = bucket;
      this.table = table;
    }

    @Override
    public void callback(String filepath) {
      // upload to GCS
      BlobInfo blobInfo = BlobInfo
          .newBuilder(bucket, filepath)
          .setAcl(new ArrayList<>(Arrays.asList(Acl.of(User.ofAllUsers(), Role.READER))))
          .build();

      // Upload to GCS
      try (FileInputStream is = new FileInputStream(new File(filepath))) {
        storage.create(blobInfo, is);

      } catch (IOException e) {
        log.error("OnFileRotation.callback", "upload to gcs", e);
        return;
      }

      // Ingest to BQ
      Job loadJob = table.load(FormatOptions.avro(), blobInfo.getSelfLink());
      try {
        loadJob.waitFor();
        if (loadJob.getStatus().getError() != null) {
          throw new RuntimeException(loadJob.getStatus().getError().getMessage());
        }

      } catch (InterruptedException e) {
        log.error("OnFileRotation.callback", "bq ingest gcs", e);

        return;

      } catch (RuntimeException e) {
        log.error("OnFileRotation.callback", "bq ingest gcs", e);
        return;
      }

      // Delete File as now done
      try {
        Files.delete(Paths.get(filepath));

      } catch (IOException e) {
        log.error("OnFileRotation.callback", "cleanup file", e);
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
