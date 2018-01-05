package com.google.blockToBq;

import com.google.blockToBq.generated.AvroBitcoinBlock;
import java.io.File;
import java.io.IOException;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AvroWriter {
  private static final Logger log = LoggerFactory.getLogger(AvroWriter.class);
  private DataFileWriter<AvroBitcoinBlock> writer;

  public AvroWriter(File file) throws IOException {
    DatumWriter<AvroBitcoinBlock> blockWriter = new SpecificDatumWriter<>(AvroBitcoinBlock.class);
    writer = new DataFileWriter<>(blockWriter);
    if (file.exists()) {
      writer.appendTo(file);
    } else {
      writer.create(AvroBitcoinBlock.getClassSchema(), file);
    }
  }

  /** Writes a {@link AvroBitcoinBlock}. */
  public synchronized void write(AvroBitcoinBlock bitcoinBlock) throws IOException {
    writer.append(bitcoinBlock);
  }

  /** Closes file. */
  public void close() throws IOException {
    writer.close();
  }
}
