package io.confluent.kafka.connect.cdc;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static io.confluent.kafka.connect.cdc.ChangeAssertions.assertColumnValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class JsonColumnValueTest {
  private static final Logger log = LoggerFactory.getLogger(JsonColumnValueTest.class);

  @Test
  public void roundTrip() throws IOException {
    Change.ColumnValue expected = mock(Change.ColumnValue.class);
    final Schema expectedSchema = SchemaBuilder.struct()
        .name("Testing")
        .field("x", Schema.FLOAT64_SCHEMA)
        .field("y", Schema.FLOAT64_SCHEMA)
        .build();
    final Struct expectedStruct = new Struct(expectedSchema)
        .put("x", 31.2)
        .put("y", 12.7);


    when(expected.schema()).thenReturn(expectedSchema);
    when(expected.value()).thenReturn(expectedStruct);
    when(expected.columnName()).thenReturn("foo");

    String s = ObjectMapperFactory.instance.writeValueAsString(expected);
    log.debug(s);
    Change.ColumnValue actual = ObjectMapperFactory.instance.readValue(s, Change.ColumnValue.class);
    assertColumnValue(expected, actual);
  }

//
//  @Test
//  public void serialize() throws IOException {
//    JsonColumnValue columnValue = new JsonColumnValue("testColumn", Schema.OPTIONAL_STRING_SCHEMA, 1L);
//  }
//
//  @Test
//  public void decimal() {
//    JsonColumnValue a = new JsonColumnValue("testColumn", Decimal.schema(0), 1L);
//    assertEquals(BigDecimal.ONE, a.value());
//  }
//
//
//  @Test
//  public void equal() {
//    JsonColumnValue a = new JsonColumnValue("testColumn", Schema.OPTIONAL_STRING_SCHEMA, 1L);
//    JsonColumnValue b = new JsonColumnValue("testColumn", Schema.OPTIONAL_STRING_SCHEMA, 1L);
//
//    assertColumnValue(a, b);
//  }
}
