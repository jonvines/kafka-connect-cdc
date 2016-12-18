package io.confluent.kafka.connect.cdc;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY, getterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class JsonChange implements Change {
  @JsonProperty
  Map<String, String> metadata;
  @JsonProperty
  Map<String, Object> sourcePartition;
  @JsonProperty
  Map<String, Object> sourceOffset;
  @JsonProperty
  String schemaName;
  @JsonProperty
  String tableName;
  @JsonProperty
  ChangeType changeType;
  @JsonProperty
  long timestamp;

  @JsonProperty
  List<ColumnValue> keyColumns = new ArrayList<>();
  @JsonProperty
  List<ColumnValue> valueColumns = new ArrayList<>();

  @Override
  public Map<String, String> metadata() {
    return this.metadata;
  }

  @Override
  public Map<String, Object> sourcePartition() {
    return this.sourcePartition;
  }

  @Override
  public Map<String, Object> sourceOffset() {
    return this.sourceOffset;
  }

  @Override
  public String schemaName() {
    return this.schemaName;
  }

  @Override
  public String tableName() {
    return this.tableName;
  }

  @Override
  public List<ColumnValue> keyColumns() {
    return this.keyColumns;
  }

  @Override
  public List<ColumnValue> valueColumns() {
    return this.valueColumns;
  }

  @Override
  public ChangeType changeType() {
    return this.changeType;
  }

  @Override
  public long timestamp() {
    return this.timestamp;
  }
}
