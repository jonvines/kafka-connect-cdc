package io.confluent.kafka.connect.cdc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static io.confluent.kafka.connect.cdc.KafkaAssert.assertSchema;
import static io.confluent.kafka.connect.cdc.KafkaAssert.assertStruct;

public class JsonStructTest {
  private static final Logger log = LoggerFactory.getLogger(JsonStructTest.class);
  @Test
  public void roundtrip() throws IOException {

    final Schema expectedSchema =SchemaBuilder.struct()
        .name("Testing")
        .field("x", Schema.FLOAT64_SCHEMA)
        .field("y", Schema.FLOAT64_SCHEMA)
        .build();
    final Struct expectedStruct = new Struct(expectedSchema)
        .put("x", 31.2)
        .put("y", 12.7)
        ;

    String buffer = ObjectMapperFactory.instance.writeValueAsString(expectedStruct);
    log.trace(buffer);
    final Struct actualStruct = ObjectMapperFactory.instance.readValue(buffer, Struct.class);
    assertStruct(expectedStruct, actualStruct);
    assertSchema(expectedSchema, actualStruct.schema());
  }


}
