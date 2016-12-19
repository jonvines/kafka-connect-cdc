package io.confluent.kafka.connect.cdc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.kafka.connect.data.Schema;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

public class JsonColumnValueTest {

  @Test
  public void serialize() throws IOException {
    JsonColumnValue columnValue = new JsonColumnValue("testColumn", Schema.OPTIONAL_STRING_SCHEMA, 1L);


    ObjectMapper mapper = new ObjectMapper();
    mapper.configure(SerializationFeature.INDENT_OUTPUT, true);
//    mapper.writeValue(System.out, columnValue);

  }

  @Test
  public void equal() {
    JsonColumnValue a = new JsonColumnValue("testColumn", Schema.OPTIONAL_STRING_SCHEMA, 1L);
    JsonColumnValue b = new JsonColumnValue("testColumn", Schema.OPTIONAL_STRING_SCHEMA, 1L);

    assertEquals(a, b);
  }

  @Test
  public void notEqual() {
    JsonColumnValue a = new JsonColumnValue("testColumn", Schema.OPTIONAL_STRING_SCHEMA, 1L);
    JsonColumnValue b = new JsonColumnValue("testColumn", Schema.STRING_SCHEMA, 1L);

    assertNotEquals(a, b);
  }
}
