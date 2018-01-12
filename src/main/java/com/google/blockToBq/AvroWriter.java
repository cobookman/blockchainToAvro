package com.google.blockToBq;

import com.google.blockToBq.generated.AvroBitcoinBlock;
import java.io.File;
import java.io.IOException;

import java.nio.file.FileSystems;
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
  private int currentHour;
  private String directory;

  public AvroWriter(String directory) throws IOException {
    this.directory = directory;
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
      Date date = new Date();
      Files.move(
          Paths.get(tempFile.getAbsolutePath()),
          Paths.get(this.directory + "done/" + dateFormat.format(date) + ".avro"));
    }

    DatumWriter<AvroBitcoinBlock> blockWriter = new SpecificDatumWriter<>(AvroBitcoinBlock.class);
    this.writer = new DataFileWriter<>(blockWriter);
    writer.create(AvroBitcoinBlock.getClassSchema(), tempFile);
    currentHour = Calendar.getInstance().get(Calendar.HOUR_OF_DAY);
  }

  private boolean needsFileRotation() {
    return currentHour != Calendar.getInstance().get(Calendar.HOUR_OF_DAY);
  }

  /** Writes a {@link AvroBitcoinBlock}. */
  public synchronized void write(AvroBitcoinBlock bitcoinBlock) throws IOException {
    if (needsFileRotation()) {
      rotateFile();
    }

    writer.append(bitcoinBlock);
  }

  /** Closes file. */
  public void close() throws IOException {
    writer.close();
  }
}
