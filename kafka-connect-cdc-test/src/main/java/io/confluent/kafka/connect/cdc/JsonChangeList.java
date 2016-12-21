package io.confluent.kafka.connect.cdc;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;


public class JsonChangeList extends ArrayList<JsonChange> {
  public static void write(File file, JsonChangeList change) throws IOException {
    try (OutputStream outputStream = new FileOutputStream(file)) {
      ObjectMapperFactory.instance.writeValue(outputStream, change);
    }
  }

  public static void write(OutputStream outputStream, JsonChangeList change) throws IOException {
    ObjectMapperFactory.instance.writeValue(outputStream, change);
  }

  public static JsonChangeList read(InputStream inputStream) throws IOException {
    return ObjectMapperFactory.instance.readValue(inputStream, JsonChangeList.class);
  }
}