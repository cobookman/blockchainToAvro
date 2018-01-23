package com.google.blockToBq;

import com.google.blockToBq.generated.AvroBitcoinBlock;
import java.io.File;
import java.io.IOException;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;

public class AvroGcsWriter {
  private String workDirectory;
  private int rotationTime;
  private Callback callback;
  private File file;
  private int timeWindowId;
  private DataFileWriter<AvroBitcoinBlock> writer;

  public AvroGcsWriter(String workDirectory, Integer rotationTime, Callback callback) throws IOException {
    this.workDirectory = workDirectory;
    this.rotationTime = rotationTime;
    if (this.workDirectory.endsWith("/")) {
      this.workDirectory = this.workDirectory.substring(0, this.workDirectory.length() -1);
    }
    this.callback = callback;
    rotate();
  }

  /** Writes a {@link AvroBitcoinBlock}. */
  public synchronized void write(AvroBitcoinBlock bitcoinBlock) throws IOException {
    if (timeWindowId != getCurrentTimeWindowId()) {
      rotate();
    }
    writer.append(bitcoinBlock);
  }

  public int getCurrentTimeWindowId() {
    return (Calendar.getInstance().get(Calendar.SECOND) / rotationTime);
  }

  /** Rotates the open file. */
  public synchronized void rotate() throws IOException {
    // Close old file & sent event to callback
    this.close();

    // Open up new file
    DateFormat dateFormat = new SimpleDateFormat("yyyy.MM.dd.HH.ss");
    String path = this.workDirectory + "/" + dateFormat.format(new Date()) + ".avro";
    this.file = new File(path);

    // Create writer for file
    DatumWriter<AvroBitcoinBlock> blockWriter = new SpecificDatumWriter<>(AvroBitcoinBlock.class);
    DataFileWriter<AvroBitcoinBlock> writer = new DataFileWriter<>(blockWriter).create(AvroBitcoinBlock.getClassSchema(), file);
    writer.create(AvroBitcoinBlock.getClassSchema(), file);
    this.writer = writer;

    // update time window id
    this.timeWindowId = getCurrentTimeWindowId();
  }

  /** Closes any open files. */
  public synchronized void close() throws IOException {
    if (writer != null) {
      this.writer.close();
      final String absPath = file.getAbsolutePath();
      Thread thread = new Thread(() -> {
        callback.callback(absPath);
      });
      thread.start();
    }
  }

  public interface Callback {
    void callback(String filepath);
  }

}
