package io.confluent.kafka.connect.cdc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import java.io.IOException;

public class JsonChangeTest {

  @Test
  public void serialize() throws IOException {
    JsonChange change = new JsonChange();
    change.metadata = ImmutableMap.of("one", "1", "two", "2");
    change.sourcePartition = ImmutableMap.of("partition", (Object)1);
    change.sourceOffset = ImmutableMap.of("testing", (Object)1);
    change.schemaName = "schemaName";
    change.tableName = "tableName";
    change.changeType = Change.ChangeType.INSERT;
    change.timestamp = 1482095102000L;


    ObjectMapper mapper = new ObjectMapper();
    mapper.configure(SerializationFeature.INDENT_OUTPUT, true);
    mapper.writeValue(System.out, change);

  }

}
