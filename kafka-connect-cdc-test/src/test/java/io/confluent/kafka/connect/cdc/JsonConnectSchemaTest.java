package io.confluent.kafka.connect.cdc;

import org.apache.kafka.connect.data.Schema;
import org.junit.Test;

import static org.junit.Assert.*;

public class JsonConnectSchemaTest {

  @Test
  public void equalsNull(){
    JsonConnectSchema a = new JsonConnectSchema(Schema.INT32_SCHEMA);
    assertFalse(a.equals(null));
  }

  @Test
  public void equals() {
    JsonConnectSchema a = new JsonConnectSchema(Schema.INT32_SCHEMA);
    JsonConnectSchema b = new JsonConnectSchema(Schema.INT32_SCHEMA);
    assertEquals(a, b);
  }

  @Test
  public void notEqual() {
    JsonConnectSchema a = new JsonConnectSchema(Schema.INT32_SCHEMA);
    JsonConnectSchema b = new JsonConnectSchema(Schema.OPTIONAL_INT32_SCHEMA);
    assertNotEquals(a, b);
  }

}
