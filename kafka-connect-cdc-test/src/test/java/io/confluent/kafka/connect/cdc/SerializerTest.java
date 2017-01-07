package io.confluent.kafka.connect.cdc;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static io.confluent.kafka.connect.cdc.KafkaAssert.assertSchema;

public class SerializerTest {
  @Test
  public void roundTrip() throws IOException {
    final Schema expected = Decimal.builder(12)
        .optional()
        .doc("This is for testing")
        .build();
    byte[] buffer = ObjectMapperFactory.instance.writeValueAsBytes(expected);
    final Schema actual = ObjectMapperFactory.instance.readValue(buffer, Schema.class);
    assertSchema(expected, actual);
  }
}
