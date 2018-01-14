package com.google.blockToBq;

import com.google.blockToBq.generated.AvroBitcoinBlock;
import com.google.common.base.Strings;
import java.io.File;
import java.io.IOException;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AvroWriter {
  private static final Logger log = LoggerFactory.getLogger(AvroWriter.class);
  private DataFileWriter<AvroBitcoinBlock> writer;
  private int timeWindowId;
  private String directory;
  private String scriptPath;

  public AvroWriter(String directory, String scriptPath) throws IOException {
    this.directory = directory;
    this.scriptPath = scriptPath;
    rotateFile();
  }

  private void rotateFile() throws IOException {
    File tempFile = new File(directory + "blocks.avro.tmp");

    if (writer != null) {
      writer.close();
    }

    if (tempFile.exists()) {
      log.debug("rotating file");

      // make 'done' folder if need be
      File doneDirectory = new File(this.directory + "done/");
      if (!doneDirectory.exists()) {
        doneDirectory.mkdir();
      }

      // move old file to 'done' folder
      DateFormat dateFormat = new SimpleDateFormat("yyyy.MM.dd.HH.ss");
      String newPath = this.directory + "done/" + dateFormat.format(new Date()) + ".avro";
      Files.move(
          Paths.get(tempFile.getAbsolutePath()),
          Paths.get(newPath));

      // call on blockScript (if not null)
      if (!Strings.isNullOrEmpty(scriptPath)) {
        try {
          new ProcessBuilder(scriptPath, newPath).start();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }

    DatumWriter<AvroBitcoinBlock> blockWriter = new SpecificDatumWriter<>(AvroBitcoinBlock.class);
    this.writer = new DataFileWriter<>(blockWriter);
    writer.create(AvroBitcoinBlock.getClassSchema(), tempFile);
    timeWindowId = getCurrentTimeWindowId();
  }

  public int getCurrentTimeWindowId() {
    return (Calendar.getInstance().get(Calendar.MINUTE) / 15);
  }

  /** Writes a {@link AvroBitcoinBlock}. */
  public synchronized void write(AvroBitcoinBlock bitcoinBlock) throws IOException {
    if (timeWindowId != getCurrentTimeWindowId()) {
      rotateFile();
    }

    writer.append(bitcoinBlock);
  }

  /** Closes file. */
  public void close() throws IOException {
    writer.close();
  }
}
