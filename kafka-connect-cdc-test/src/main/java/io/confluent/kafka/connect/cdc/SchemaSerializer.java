package io.confluent.kafka.connect.cdc;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.kafka.connect.data.Schema;

import java.io.IOException;

class SchemaSerializer extends JsonSerializer<Schema> {
  @Override
  public void serialize(Schema schema, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException, JsonProcessingException {
    JsonConnectSchema jsonConnectSchema = new JsonConnectSchema(schema);
    jsonGenerator.writeObject(jsonConnectSchema);
  }
}
