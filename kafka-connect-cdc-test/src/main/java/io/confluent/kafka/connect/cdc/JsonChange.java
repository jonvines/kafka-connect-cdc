package io.confluent.kafka.connect.cdc;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.google.common.base.MoreObjects;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY, getterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class JsonChange {
  Map<String, String> metadata = new LinkedHashMap<>();
  Map<String, Object> sourcePartition = new LinkedHashMap<>();
  Map<String, Object> sourceOffset = new LinkedHashMap<>();
  String databaseName;
  String schemaName;
  String tableName;
  Change.ChangeType changeType;
  long timestamp;
  @JsonProperty
  List<Change.ColumnValue> keyColumns = new ArrayList<>();
  @JsonProperty
  List<Change.ColumnValue> valueColumns = new ArrayList<>();

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

  static class Serializer extends JsonSerializer<Change> {
    @Override
    public void serialize(Change change, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException, JsonProcessingException {
      JsonChange result = new JsonChange();
      result.databaseName = change.databaseName();
      result.schemaName = change.schemaName();
      result.tableName = change.tableName();
      result.changeType = change.changeType();
      result.timestamp = change.timestamp();
      result.metadata = change.metadata();
      result.sourceOffset = change.sourceOffset();
      result.sourcePartition = change.sourcePartition();
      result.keyColumns = change.keyColumns();
      result.valueColumns = change.valueColumns();
      jsonGenerator.writeObject(result);
    }
  }

  static class Deserializer extends JsonDeserializer<Change> {
    @Override
    public Change deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
      JsonChange storage = jsonParser.readValueAs(JsonChange.class);
      Change result = mock(Change.class);
      when(result.databaseName()).thenReturn(storage.databaseName);
      when(result.schemaName()).thenReturn(storage.schemaName);
      when(result.tableName()).thenReturn(storage.tableName);
      when(result.changeType()).thenReturn(storage.changeType);
      when(result.timestamp()).thenReturn(storage.timestamp);
      when(result.metadata()).thenReturn(storage.metadata);
      when(result.sourceOffset()).thenReturn(storage.sourceOffset);
      when(result.sourcePartition()).thenReturn(storage.sourcePartition);
      when(result.keyColumns()).thenReturn(storage.keyColumns);
      when(result.valueColumns()).thenReturn(storage.valueColumns);
      return result;
    }
  }
}
