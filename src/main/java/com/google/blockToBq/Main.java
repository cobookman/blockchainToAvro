package com.google.blockToBq;

import com.google.common.base.Strings;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
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
      throws InterruptedException, ExecutionException, BlockStoreException, IOException {
    Options options = new Options();

    Option blockScript = new Option("s", "script", true, "script to run on a new block");
    blockScript.setRequired(false);
    options.addOption(blockScript);

    Option workDir = new Option("d", "directory", true, "where to save data");
    workDir.setRequired(false);
    options.addOption(workDir);

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

    sync(cmd);
  }

  public static void sync(CommandLine cmd)
      throws IOException, ExecutionException, InterruptedException, BlockStoreException {

    attachShutdownListener();
    NetworkParameters networkParameters = new MainNetParams();

    String filePrefix;
    if (!Strings.isNullOrEmpty(cmd.getOptionValue("directory"))) {
      filePrefix = cmd.getOptionValue("directory");
    } else {
      filePrefix = System.getProperty("user.dir") + "/";
    }

    writer = new AvroWriter(filePrefix, cmd.getOptionValue("script"));
    bitcoinBlockHandler = new BitcoinBlockHandler(writer);
    bitcoinBlockDownloader = new BitcoinBlockDownloader();
    bitcoinBlockDownloader.start(networkParameters, bitcoinBlockHandler);
    while (true) {
      if (bitcoinBlockDownloader.isDone()) {
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
    if (bitcoinBlockDownloader != null) {
      System.out.println("download of blockchain:\tstopping");
      bitcoinBlockDownloader.stop();
      System.out.println("download of blockchain:\tstopped");
    }

    if (bitcoinBlockHandler != null) {
      System.out.println("block queue:\tfinishing");
      bitcoinBlockHandler.stop();
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
