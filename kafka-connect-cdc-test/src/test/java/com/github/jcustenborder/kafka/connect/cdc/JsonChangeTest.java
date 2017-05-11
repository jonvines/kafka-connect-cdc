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

public class JsonChangeTest {

//  @Test
//  public void serialize() throws IOException {
//    JsonChange expected = new JsonChange();
//    expected.metadata = ImmutableMap.of("one", "1", "two", "2");
//    expected.sourcePartition = ImmutableMap.of("partition", (Object) 1L);
//    expected.sourceOffset = ImmutableMap.of("testing", (Object) 1L);
//    expected.schemaName = "schemaName";
//    expected.tableName = "tableName";
//    expected.changeType = Change.ChangeType.INSERT;
//    expected.timestamp = 1482095102000L;
//
//    expected.keyColumns.add(
//        new JsonColumnValue("user_id", Schema.INT32_SCHEMA, 1)
//    );
//    expected.valueColumns.add(
//        new JsonColumnValue("user_id", Schema.INT32_SCHEMA, 1)
//    );
//
//
//    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
//    ObjectMapperFactory.INSTANCE.writeValue(outputStream, expected);
//    byte[] buffer = outputStream.toByteArray();
//    JsonChange actual = ObjectMapperFactory.INSTANCE.readValue(buffer, JsonChange.class);
//
//    assertChange(expected, actual);
//  }
//
//  @Test
//  public void equals() {
//    JsonChange thisChange = new JsonChange();
//    thisChange.metadata = ImmutableMap.of("one", "1", "two", "2");
//    thisChange.sourcePartition = ImmutableMap.of("partition", (Object) 1);
//    thisChange.sourceOffset = ImmutableMap.of("testing", (Object) 1);
//    thisChange.schemaName = "schemaName";
//    thisChange.tableName = "tableName";
//    thisChange.changeType = Change.ChangeType.INSERT;
//    thisChange.timestamp = 1482095102000L;
//
//    thisChange.keyColumns.add(
//        new JsonColumnValue("user_id", Schema.INT32_SCHEMA, 1)
//    );
//    thisChange.valueColumns.add(
//        new JsonColumnValue("user_id", Schema.INT32_SCHEMA, 1)
//    );
//
//    JsonChange thatChange = new JsonChange();
//    thatChange.metadata = ImmutableMap.of("one", "1", "two", "2");
//    thatChange.sourcePartition = ImmutableMap.of("partition", (Object) 1);
//    thatChange.sourceOffset = ImmutableMap.of("testing", (Object) 1);
//    thatChange.schemaName = "schemaName";
//    thatChange.tableName = "tableName";
//    thatChange.changeType = Change.ChangeType.INSERT;
//    thatChange.timestamp = 1482095102000L;
//
//    thatChange.keyColumns.add(
//        new JsonColumnValue("user_id", Schema.INT32_SCHEMA, 1)
//    );
//    thatChange.valueColumns.add(
//        new JsonColumnValue("user_id", Schema.INT32_SCHEMA, 1)
//    );
//
//    assertChange(thisChange, thatChange);
//  }
//
//  @Test
//  public void notEquals() {
//
//
//  }


}
