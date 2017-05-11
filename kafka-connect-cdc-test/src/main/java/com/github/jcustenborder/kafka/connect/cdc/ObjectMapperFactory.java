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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

public class ObjectMapperFactory {

  public static final ObjectMapper INSTANCE;

  static {
    INSTANCE = new ObjectMapper();
    INSTANCE.configure(SerializationFeature.FLUSH_AFTER_WRITE_VALUE, true);
    INSTANCE.configure(SerializationFeature.INDENT_OUTPUT, true);
    INSTANCE.configure(SerializationFeature.WRITE_NULL_MAP_VALUES, true);
    INSTANCE.configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true);
    INSTANCE.configure(DeserializationFeature.USE_LONG_FOR_INTS, true);
    INSTANCE.configure(DeserializationFeature.FAIL_ON_UNRESOLVED_OBJECT_IDS, true);

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

    INSTANCE.registerModule(schemaModule);
  }


}
