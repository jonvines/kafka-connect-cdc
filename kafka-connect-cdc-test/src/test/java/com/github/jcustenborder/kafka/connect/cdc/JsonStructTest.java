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

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.github.jcustenborder.kafka.connect.cdc.KafkaAssert.assertSchema;
import static com.github.jcustenborder.kafka.connect.cdc.KafkaAssert.assertStruct;

public class JsonStructTest {
  private static final Logger log = LoggerFactory.getLogger(JsonStructTest.class);

  @Test
  public void roundtrip() throws IOException {

    final Schema expectedSchema = SchemaBuilder.struct()
        .name("Testing")
        .field("x", Schema.FLOAT64_SCHEMA)
        .field("y", Schema.FLOAT64_SCHEMA)
        .build();
    final Struct expectedStruct = new Struct(expectedSchema)
        .put("x", 31.2)
        .put("y", 12.7);

    String buffer = ObjectMapperFactory.INSTANCE.writeValueAsString(expectedStruct);
    log.trace(buffer);
    final Struct actualStruct = ObjectMapperFactory.INSTANCE.readValue(buffer, Struct.class);
    assertStruct(expectedStruct, actualStruct);
    assertSchema(expectedSchema, actualStruct.schema());
  }


}
