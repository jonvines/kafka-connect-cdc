package io.confluent.kafka.connect.cdc;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;
import oracle.sql.Datum;
import org.apache.kafka.connect.data.Schema;

public class ObjectMapperFactory {

  public static final ObjectMapper instance;

  static {
    instance = new ObjectMapper();
    instance.configure(SerializationFeature.FLUSH_AFTER_WRITE_VALUE, true);
    instance.configure(SerializationFeature.INDENT_OUTPUT, true);
    instance.configure(SerializationFeature.WRITE_NULL_MAP_VALUES, false);
    instance.configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true);
    instance.configure(DeserializationFeature.USE_LONG_FOR_INTS, true);
    instance.configure(DeserializationFeature.FAIL_ON_UNRESOLVED_OBJECT_IDS, true);
//    instance.findAndRegisterModules();
    SimpleModule schemaModule = new SimpleModule();
    schemaModule.addSerializer(Schema.class, new JsonConnectSchema.SchemaSerializer());
    schemaModule.addDeserializer(Schema.class, new SchemaDeserializer());
    instance.registerModule(schemaModule);

    SimpleModule datumModule = new SimpleModule();
    datumModule.addDeserializer(Datum.class, new JsonDatum.DatumDeserializer());
    datumModule.addSerializer(Datum.class, new JsonDatum.DatumSerializer());
    instance.registerModule(datumModule);

  }


}
