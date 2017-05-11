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
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.google.common.base.Preconditions;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import java.io.IOException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY, getterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class JsonColumnValue {
  String columnName;
  Schema schema;

  Struct struct;
  Object value;


  void value(Object value) {
    switch (this.schema.type()) {
      case STRUCT:
        Preconditions.checkState(value instanceof Struct, "value must be a struct.");
        this.struct = (Struct) value;
        break;
      default:
        this.value = value;
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
        result = ValueHelper.value(this.schema, this.value);
        break;
    }

    return result;
  }

  static class Serializer extends JsonSerializer<Change.ColumnValue> {
    @Override
    public void serialize(Change.ColumnValue columnValue, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException {
      JsonColumnValue result = new JsonColumnValue();
      result.columnName = columnValue.columnName();
      result.schema = columnValue.schema();
      result.value(columnValue.value());
      jsonGenerator.writeObject(result);
    }
  }

  static class Deserializer extends JsonDeserializer<Change.ColumnValue> {
    @Override
    public Change.ColumnValue deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
      final JsonColumnValue storage = jsonParser.readValueAs(JsonColumnValue.class);
      Change.ColumnValue result = mock(Change.ColumnValue.class);
      when(result.columnName()).thenReturn(storage.columnName);
      when(result.schema()).thenReturn(storage.schema);
      when(result.value()).thenAnswer(invocationOnMock -> storage.value());
      return result;
    }
  }

}
