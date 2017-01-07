package io.confluent.kafka.connect.cdc;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

public class ObjectMapperFactory {

  public static final ObjectMapper instance;

  static {
    instance = new ObjectMapper();
    instance.configure(SerializationFeature.FLUSH_AFTER_WRITE_VALUE, true);
    instance.configure(SerializationFeature.INDENT_OUTPUT, true);
    instance.configure(SerializationFeature.WRITE_NULL_MAP_VALUES, true);
    instance.configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true);
    instance.configure(DeserializationFeature.USE_LONG_FOR_INTS, true);
    instance.configure(DeserializationFeature.FAIL_ON_UNRESOLVED_OBJECT_IDS, true);

    SimpleModule schemaModule = new SimpleModule();
    schemaModule.addSerializer(Schema.class, new JsonConnectSchema.Serializer());
    schemaModule.addDeserializer(Schema.class, new JsonConnectSchema.Deserializer());

    schemaModule.addSerializer(Struct.class, new JsonStruct.Serializer());
    schemaModule.addDeserializer(Struct.class, new JsonStruct.Deserializer());

    schemaModule.addSerializer(TableMetadataProvider.TableMetadata.class, new JsonTableMetadata.Serializer());
    schemaModule.addDeserializer(TableMetadataProvider.TableMetadata.class, new JsonTableMetadata.Deserializer());

    schemaModule.addSerializer(Change.ColumnValue.class, new JsonColumnValue.Serializer());
    schemaModule.addDeserializer(Change.ColumnValue.class, new JsonColumnValue.Deserializer());

    schemaModule.addSerializer(Change.class, new JsonChange.Serializer());
    schemaModule.addDeserializer(Change.class, new JsonChange.Deserializer());

    schemaModule.addSerializer(Time.class, new JsonTime.Serializer());
    schemaModule.addDeserializer(Time.class, new JsonTime.Deserializer());

    instance.registerModule(schemaModule);
  }


}
