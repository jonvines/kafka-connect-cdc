package io.confluent.kafka.connect.cdc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.collect.ImmutableMap;
import org.apache.kafka.connect.data.Schema;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class JsonChangeTest {

  @Test
  public void serialize() throws IOException {
    JsonChange expected = new JsonChange();
    expected.metadata = ImmutableMap.of("one", "1", "two", "2");
    expected.sourcePartition = ImmutableMap.of("partition", (Object) 1);
    expected.sourceOffset = ImmutableMap.of("testing", (Object) 1);
    expected.schemaName = "schemaName";
    expected.tableName = "tableName";
    expected.changeType = Change.ChangeType.INSERT;
    expected.timestamp = 1482095102000L;

    expected.keyColumns.add(
        new JsonColumnValue("user_id", Schema.INT32_SCHEMA, 1)
    );
    expected.valueColumns.add(
        new JsonColumnValue("user_id", Schema.INT32_SCHEMA, 1)
    );

    ObjectMapper mapper = new ObjectMapper();
    mapper.configure(SerializationFeature.INDENT_OUTPUT, true);
    mapper.configure(SerializationFeature.FLUSH_AFTER_WRITE_VALUE, true);

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    mapper.writeValue(outputStream, expected);
    byte[] buffer = outputStream.toByteArray();
    JsonChange actual = mapper.readValue(buffer, JsonChange.class);

    assertEquals(expected, actual);
  }

  @Test
  public void equals() {
    JsonChange thisChange = new JsonChange();
    thisChange.metadata = ImmutableMap.of("one", "1", "two", "2");
    thisChange.sourcePartition = ImmutableMap.of("partition", (Object) 1);
    thisChange.sourceOffset = ImmutableMap.of("testing", (Object) 1);
    thisChange.schemaName = "schemaName";
    thisChange.tableName = "tableName";
    thisChange.changeType = Change.ChangeType.INSERT;
    thisChange.timestamp = 1482095102000L;

    thisChange.keyColumns.add(
        new JsonColumnValue("user_id", Schema.INT32_SCHEMA, 1)
    );
    thisChange.valueColumns.add(
        new JsonColumnValue("user_id", Schema.INT32_SCHEMA, 1)
    );

    JsonChange thatChange = new JsonChange();
    thatChange.metadata = ImmutableMap.of("one", "1", "two", "2");
    thatChange.sourcePartition = ImmutableMap.of("partition", (Object) 1);
    thatChange.sourceOffset = ImmutableMap.of("testing", (Object) 1);
    thatChange.schemaName = "schemaName";
    thatChange.tableName = "tableName";
    thatChange.changeType = Change.ChangeType.INSERT;
    thatChange.timestamp = 1482095102000L;

    thatChange.keyColumns.add(
        new JsonColumnValue("user_id", Schema.INT32_SCHEMA, 1)
    );
    thatChange.valueColumns.add(
        new JsonColumnValue("user_id", Schema.INT32_SCHEMA, 1)
    );

    assertEquals(thisChange, thatChange);
  }

  @Test
  public void notEquals() {


  }


}
