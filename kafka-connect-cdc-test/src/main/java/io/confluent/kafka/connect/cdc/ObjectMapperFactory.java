package io.confluent.kafka.connect.cdc;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.kafka.connect.data.Schema;

public class ObjectMapperFactory {

  public static final ObjectMapper instance;

  static {
    instance = new ObjectMapper();
    instance.configure(SerializationFeature.FLUSH_AFTER_WRITE_VALUE, true);
    instance.configure(SerializationFeature.INDENT_OUTPUT, true);
    instance.configure(SerializationFeature.WRITE_NULL_MAP_VALUES, false);
    instance.configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true);

    SimpleModule simpleModule = new SimpleModule();
    simpleModule.addSerializer(Schema.class, new SchemaSerializer());
    simpleModule.addDeserializer(Schema.class, new SchemaDeserializer());
    instance.registerModule(simpleModule);
  }




}
