package com.google.blockToBq;

import com.google.api.core.ApiFuture;
import com.google.blockToBq.generated.AvroBitcoinBlock;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.storage.Acl;
import com.google.cloud.storage.Acl.Role;
import com.google.cloud.storage.Acl.User;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.gson.Gson;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AvroGcsWriter {
  private Publisher pubsub;
  private Storage storage;
  private String bucket;
  private Gson gson;

  public AvroGcsWriter(Publisher pubsub, Storage storage, String bucket) throws IOException {
    this.pubsub = pubsub;
    this.storage = storage;
    this.bucket = bucket;
    this.gson = new Gson();
  }

  /** Writes a {@link AvroBitcoinBlock}. */
  public void write(AvroBitcoinBlock bitcoinBlock)
      throws IOException, ExecutionException, InterruptedException {
    String fileName = bitcoinBlock.getBlockId().toString() + ".avro";

    BlobInfo blobInfo = BlobInfo
        .newBuilder(bucket, fileName)
        .setAcl(new ArrayList<>(Arrays.asList(Acl.of(User.ofAllUsers(), Role.READER))))
        .build();

    // write data to GCS
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(os, null);
    DatumWriter<AvroBitcoinBlock> writer = new SpecificDatumWriter<>(AvroBitcoinBlock.class);
    writer.write(bitcoinBlock, encoder);
    encoder.flush();
    os.close();
    storage.create(blobInfo, os.toByteArray());

    // notify topic of written file
    HashMap<String, String> msgData = new HashMap<>();
    msgData.put("gcsPath", "gs://" + bucket + "/" + fileName);
    msgData.put("blockId", bitcoinBlock.getBlockId().toString());
    msgData.put("blockTimestamp", bitcoinBlock.getTimestamp().toString());
    msgData.put("previousBlockId", bitcoinBlock.getPreviousBlock().toString());

    ApiFuture<String> future = this.pubsub.publish(PubsubMessage.newBuilder()
        .setData(ByteString.copyFromUtf8(gson.toJson(msgData)))
        .build());
    future.get();
  }

  /** Closes any open files. */
  public void close() throws IOException {
    // do nothing. Used in case one day we change to writing to a file system or batching writes.
  }
}
