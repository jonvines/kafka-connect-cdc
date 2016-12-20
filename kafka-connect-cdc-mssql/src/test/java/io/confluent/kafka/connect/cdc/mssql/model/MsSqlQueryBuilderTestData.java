package io.confluent.kafka.connect.cdc.mssql.model;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import io.confluent.kafka.connect.cdc.JsonChange;
import io.confluent.kafka.connect.cdc.JsonTableMetadata;
import io.confluent.kafka.connect.cdc.JsonTime;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY, getterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class MsSqlQueryBuilderTestData {
  JsonTableMetadata tableMetadata;
  JsonTime time;
  JsonChange expected;

  public JsonChange expected() {
    return this.expected;
  }

  public void expected(JsonChange expected) {
    this.expected = expected;
  }

  public JsonTableMetadata tableMetadata() {
    return this.tableMetadata;
  }

  public void tableMetadata(JsonTableMetadata tableMetadata) {
    this.tableMetadata = tableMetadata;
  }

  public JsonTime time() {
    return this.time;
  }

  public void time(JsonTime time) {
    this.time = time;
  }
}
