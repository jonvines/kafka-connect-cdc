package io.confluent.kafka.connect.cdc;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.MoreObjects;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY, getterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class JsonChange implements Change {
  @JsonProperty
  Map<String, String> metadata = new LinkedHashMap<>();
  @JsonProperty
  Map<String, Object> sourcePartition = new LinkedHashMap<>();
  @JsonProperty
  Map<String, Object> sourceOffset = new LinkedHashMap<>();
  @JsonProperty
  String databaseName;
  @JsonProperty
  String schemaName;
  @JsonProperty
  String tableName;
  @JsonProperty
  ChangeType changeType;
  @JsonProperty
  long timestamp;

  @JsonDeserialize(contentAs = JsonColumnValue.class)
  @JsonProperty
  List<ColumnValue> keyColumns = new ArrayList<>();

  @JsonDeserialize(contentAs = JsonColumnValue.class)
  @JsonProperty
  List<ColumnValue> valueColumns = new ArrayList<>();

  public static void write(File file, JsonChange change) throws IOException {
    try (OutputStream outputStream = new FileOutputStream(file)) {
      ObjectMapperFactory.instance.writeValue(outputStream, change);
    }
  }

  public static void write(OutputStream outputStream, JsonChange change) throws IOException {
    ObjectMapperFactory.instance.writeValue(outputStream, change);
  }

  public static JsonChange read(InputStream inputStream) throws IOException {
    return ObjectMapperFactory.instance.readValue(inputStream, JsonChange.class);
  }

  public static JsonChange convert(Change change) {
    JsonChange jsonChange = new JsonChange();
    jsonChange.sourceOffset = change.sourceOffset();
    jsonChange.sourcePartition = change.sourcePartition();
    jsonChange.metadata = change.metadata();
    jsonChange.changeType = change.changeType();
    jsonChange.databaseName = change.databaseName();
    jsonChange.schemaName = change.schemaName();
    jsonChange.tableName = change.tableName();
    jsonChange.timestamp = change.timestamp();

    for (Change.ColumnValue columnValue : change.valueColumns()) {
      JsonColumnValue jsonColumnValue = JsonColumnValue.convert(columnValue);
      jsonChange.valueColumns().add(jsonColumnValue);
    }
    for (Change.ColumnValue columnValue : change.keyColumns()) {
      JsonColumnValue jsonColumnValue = JsonColumnValue.convert(columnValue);
      jsonChange.keyColumns().add(jsonColumnValue);
    }
    return jsonChange;
  }

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
  public String databaseName() {
    return this.databaseName;
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

  boolean equals(List<ColumnValue> thisList, List<ColumnValue> thatList) {
    if (thisList.size() != thatList.size()) {
      return false;
    }

    for (int i = 0; i < thisList.size(); i++) {
      ColumnValue thisColumnValue = thisList.get(i);
      ColumnValue thatColumnValue = thatList.get(i);
      if (!thisColumnValue.equals(thatColumnValue)) {
        return false;
      }
    }


    return true;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof Change)) {
      return false;
    }

    Change that = (Change) obj;

    if (!this.changeType().equals(that.changeType()))
      return false;
    if (this.timestamp() != that.timestamp()) {
      return false;
    }
    if (!this.schemaName().equals(that.schemaName()))
      return false;
    if (!this.tableName().equals(that.tableName()))
      return false;
    MapDifference<String, String> metadataDifference = Maps.difference(this.metadata(), that.metadata());
    if (!metadataDifference.areEqual())
      return false;
    MapDifference<String, Object> sourceOffsetDifference = Maps.difference(this.sourceOffset(), that.sourceOffset());
    if (!sourceOffsetDifference.areEqual())
      return false;
    MapDifference<String, Object> sourcePartitionDifference = Maps.difference(this.sourcePartition(), that.sourcePartition());
    if (!sourcePartitionDifference.areEqual())
      return false;

    if (!equals(this.keyColumns(), that.keyColumns())) {
      return false;
    }

    if (!equals(this.valueColumns(), that.valueColumns())) {
      return false;
    }

    return true;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this.getClass())
        .add("schemaName", this.schemaName)
        .add("tableName", this.tableName)
        .add("changeType", this.changeType)
        .add("metadata", this.metadata)
        .add("sourcePartition", this.sourcePartition)
        .add("sourceOffset", this.sourceOffset)
        .add("timestamp", this.timestamp)
        .add("keyColumns", this.keyColumns)
        .add("valueColumns", this.valueColumns)

        .omitNullValues()
        .toString();
  }

  public void changeType(ChangeType value) {
    this.changeType = value;
  }

  public void tableName(String value) {
    this.tableName = value;
  }

  public void schemaName(String value) {
    this.schemaName = value;
  }

  public void timestamp(long value) {
    this.timestamp = value;
  }
}
