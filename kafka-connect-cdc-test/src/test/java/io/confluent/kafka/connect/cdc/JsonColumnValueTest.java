package io.confluent.kafka.connect.cdc;

import org.apache.kafka.connect.data.Schema;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static io.confluent.kafka.connect.cdc.ChangeAssertions.assertColumnValue;
import static org.junit.Assert.assertNotEquals;

public class JsonColumnValueTest {

  @Test
  public void serialize() throws IOException {
    JsonColumnValue columnValue = new JsonColumnValue("testColumn", Schema.OPTIONAL_STRING_SCHEMA, 1L);
  }

  @Test
  public void equal() {
    JsonColumnValue a = new JsonColumnValue("testColumn", Schema.OPTIONAL_STRING_SCHEMA, 1L);
    JsonColumnValue b = new JsonColumnValue("testColumn", Schema.OPTIONAL_STRING_SCHEMA, 1L);

    assertColumnValue(a, b);
  }

  @Disabled
  @Test
  public void notEqual() {
    JsonColumnValue a = new JsonColumnValue("testColumn", Schema.OPTIONAL_STRING_SCHEMA, 1L);
    JsonColumnValue b = new JsonColumnValue("testColumn", Schema.STRING_SCHEMA, 1L);

    assertNotEquals(a, b);
  }
}
