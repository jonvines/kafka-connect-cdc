package io.confluent.kafka.connect.cdc;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.kafka.connect.data.Schema;

import java.io.IOException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY, getterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class JsonColumnValue {
  String columnName;
  Schema schema;
  Object value;

  void value(Object value) {
    this.value = value;
  }

  Object value() {
    return ValueHelper.value(this.schema, this.value);
  }

  static class Serializer extends JsonSerializer<Change.ColumnValue> {
    @Override
    public void serialize(Change.ColumnValue columnValue, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException, JsonProcessingException {
      JsonColumnValue result = new JsonColumnValue();
      result.columnName = columnValue.columnName();
      result.schema = columnValue.schema();
      result.value(columnValue.value());
      jsonGenerator.writeObject(result);
    }
  }

  static class Deserializer extends JsonDeserializer<Change.ColumnValue> {
    @Override
    public Change.ColumnValue deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
      JsonColumnValue storage = jsonParser.readValueAs(JsonColumnValue.class);
      Change.ColumnValue result = mock(Change.ColumnValue.class);
      when(result.columnName()).thenReturn(storage.columnName);
      when(result.schema()).thenReturn(storage.schema);
      when(result.value()).thenReturn(storage.value());
      return result;
    }
  }

}
