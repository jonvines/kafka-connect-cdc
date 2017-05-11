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

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static com.github.jcustenborder.kafka.connect.cdc.KafkaAssert.assertSchema;

public class SerializerTest {
  @Test
  public void roundTrip() throws IOException {
    final Schema expected = Decimal.builder(12)
        .optional()
        .doc("This is for testing")
        .build();
    byte[] buffer = ObjectMapperFactory.INSTANCE.writeValueAsBytes(expected);
    final Schema actual = ObjectMapperFactory.INSTANCE.readValue(buffer, Schema.class);
    assertSchema(expected, actual);
  }
}
