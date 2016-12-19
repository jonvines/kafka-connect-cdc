package io.confluent.kafka.connect.cdc;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import org.apache.kafka.connect.data.Schema;

import java.io.IOException;

class SchemaDeserializer extends JsonDeserializer<Schema> {

  @Override
  public Schema deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
    JsonConnectSchema jsonConnectSchema = jsonParser.readValueAs(JsonConnectSchema.class);
    return jsonConnectSchema.build();
  }
}
