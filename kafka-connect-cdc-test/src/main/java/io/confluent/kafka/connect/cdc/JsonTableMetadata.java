package io.confluent.kafka.connect.cdc;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.kafka.connect.data.Schema;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY, getterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class JsonTableMetadata {
  String databaseName;
  String schemaName;
  String tableName;
  Set<String> keyColumns;
  Map<String, Schema> columnSchemas;

  static class Serializer extends JsonSerializer<TableMetadataProvider.TableMetadata> {
    @Override
    public void serialize(TableMetadataProvider.TableMetadata tableMetadata, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException, JsonProcessingException {
      JsonTableMetadata storage = new JsonTableMetadata();
      storage.databaseName = tableMetadata.databaseName();
      storage.tableName = tableMetadata.tableName();
      storage.schemaName = tableMetadata.schemaName();
      storage.columnSchemas = tableMetadata.columnSchemas();
      storage.keyColumns = tableMetadata.keyColumns();
      jsonGenerator.writeObject(storage);
    }
  }

  static class Deserializer extends JsonDeserializer<TableMetadataProvider.TableMetadata> {
    @Override
    public TableMetadataProvider.TableMetadata deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
      JsonTableMetadata storage = jsonParser.readValueAs(JsonTableMetadata.class);
      TableMetadataProvider.TableMetadata result = mock(TableMetadataProvider.TableMetadata.class);
      when(result.databaseName()).thenReturn(storage.databaseName);
      when(result.schemaName()).thenReturn(storage.schemaName);
      when(result.tableName()).thenReturn(storage.tableName);
      when(result.columnSchemas()).thenReturn(storage.columnSchemas);
      when(result.keyColumns()).thenReturn(storage.keyColumns);
      return result;
    }
  }
}
