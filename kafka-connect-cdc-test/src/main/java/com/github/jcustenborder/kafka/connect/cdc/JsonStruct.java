/**
 * Copyright © 2017 Jeremy Custenborder (jcustenborder@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jcustenborder.kafka.connect.cdc;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.google.common.base.Preconditions;
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

  public static Struct struct(JsonStruct storage) {
    Struct struct = new Struct(storage.schema);
    for (FieldValue fieldValue : storage.fieldValues) {
      struct.put(fieldValue.name, fieldValue.value());
    }
    struct.validate();
    return struct;
  }

  @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY, getterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
  public static class FieldValue {
    String name;
    Schema schema;
    private Object storage;
    private Struct struct;

    void value(Object value) {
      switch (this.schema.type()) {
        case STRUCT:
          Preconditions.checkState(value instanceof Struct, "value must be a struct.");
          this.struct = (Struct) value;
          break;
        default:
          this.storage = value;
          break;
      }
    }

    Object value() {
      Object result;

      switch (this.schema.type()) {
        case STRUCT:
          result = this.struct;
          break;
        default:
          result = ValueHelper.value(this.schema, this.storage);
          break;
      }

      return result;
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
      Struct struct = struct(storage);
      return struct;
    }
  }


}
