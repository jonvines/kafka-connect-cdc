package io.confluent.kafka.connect.cdc;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY, getterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class JsonStruct {

  Schema schema;
  List<FieldValue> fieldValues;

  @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY, getterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
  public static class FieldValue {
    String name;
    Schema schema;
    private Object storage;

    void value(Object value) {
      this.storage = value;
    }

    Object value() {
      return ValueHelper.value(this.schema, this.storage);
    }
  }

  static class Serializer extends JsonSerializer<Struct> {
    @Override
    public void serialize(Struct struct, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException, JsonProcessingException {
      struct.validate();
      JsonStruct result = new JsonStruct();
      result.schema = struct.schema();
      result.fieldValues = new ArrayList<>();
      for (Field field : struct.schema().fields()) {
        FieldValue fieldValue = new FieldValue();
        fieldValue.name = field.name();
        fieldValue.schema = field.schema();
        fieldValue.value(struct.get(field));
        result.fieldValues.add(fieldValue);
      }
      jsonGenerator.writeObject(result);
    }
  }

  static class Deserializer extends JsonDeserializer<Struct> {
    @Override
    public Struct deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
      JsonStruct storage = jsonParser.readValueAs(JsonStruct.class);
      Struct struct = new Struct(storage.schema);
      for (FieldValue fieldValue : storage.fieldValues) {
        struct.put(fieldValue.name, fieldValue.value());
      }
      struct.validate();
      return struct;
    }
  }

}
