package io.confluent.kafka.connect.cdc;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;

import static io.confluent.kafka.connect.cdc.ChangeAssertions.assertColumnValue;
import static org.junit.jupiter.api.Assertions.assertEquals;


public class JsonColumnValueTest {
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
