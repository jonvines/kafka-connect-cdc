package io.confluent.kafka.connect.cdc;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static io.confluent.kafka.connect.cdc.ChangeAssertions.assertSchema;

public class SchemaSerializerTest {
  @Test
  public void roundTrip() throws IOException {
    final Schema expected = Decimal.builder(12)
        .optional()
        .doc("This is for testing")
        .build();

    byte[] buffer;
    try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
      ObjectMapperFactory.instance.writeValue(outputStream, expected);
      outputStream.flush();
      buffer = outputStream.toByteArray();
    }

    final Schema actual = ObjectMapperFactory.instance.readValue(buffer, Schema.class);
    assertSchema(expected, actual);
  }
}
