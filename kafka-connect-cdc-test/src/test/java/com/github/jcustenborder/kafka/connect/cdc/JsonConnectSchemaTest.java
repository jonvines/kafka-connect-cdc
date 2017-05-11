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

import com.github.jcustenborder.kafka.connect.cdc.JsonConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.junit.jupiter.api.Test;

import static org.junit.Assert.*;

public class JsonConnectSchemaTest {

  @Test
  public void equalsNull() {
    JsonConnectSchema a = new JsonConnectSchema(Schema.INT32_SCHEMA);
    assertFalse(a.equals(null));
  }

  @Test
  public void equals() {
    JsonConnectSchema a = new JsonConnectSchema(Schema.INT32_SCHEMA);
    JsonConnectSchema b = new JsonConnectSchema(Schema.INT32_SCHEMA);
    assertEquals(a, b);
  }

  @Test
  public void notEqual() {
    JsonConnectSchema a = new JsonConnectSchema(Schema.INT32_SCHEMA);
    JsonConnectSchema b = new JsonConnectSchema(Schema.OPTIONAL_INT32_SCHEMA);
    assertNotEquals(a, b);
  }

}
