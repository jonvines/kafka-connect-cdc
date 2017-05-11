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


import com.google.common.base.Strings;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import java.util.List;
import java.util.Map;

import static com.github.jcustenborder.kafka.connect.cdc.KafkaAssert.assertStruct;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ChangeAssertions {

  static void assertMap(Map<String, ?> expected, Map<String, ?> actual, String message) {
    if (null == expected && null == actual) {
      return;
    }

    String prefix = Strings.isNullOrEmpty(message) ? "" : message + ": ";
    assertNotNull(expected, prefix + "expected cannot be null");
    assertNotNull(actual, prefix + "actual cannot be null");
    MapDifference<String, ?> mapDifference = Maps.difference(expected, actual);
    assertTrue(mapDifference.areEqual(), new MapDifferenceSupplier(mapDifference, prefix));
  }

  public static void assertColumnValue(Change.ColumnValue expected, Change.ColumnValue actual) {
    assertColumnValue(expected, actual, null);
  }

  public static void assertColumnValue(Change.ColumnValue expected, Change.ColumnValue actual, String message) {
    String prefix = Strings.isNullOrEmpty(message) ? "" : message + ": ";
    assertNotNull(expected, prefix + "expected should not be null.");
    assertNotNull(actual, prefix + "actual should not be null.");
    assertEquals(expected.columnName(), actual.columnName(), prefix + "columnName should match.");
    assertEquals(expected.value(), actual.value(), prefix + "value should match.");
    KafkaAssert.assertSchema(expected.schema(), actual.schema(), prefix + "schema should match.");
  }


  static void assertColumns(List<Change.ColumnValue> expected, List<Change.ColumnValue> actual, String method) {
    assertNotNull(expected, String.format("expected.%s() cannot be null.", method));
    assertNotNull(actual, String.format("actual.%s() cannot be null.", method));

    assertEquals(expected.size(), actual.size(), String.format("expected.%s() and actual.%s() do not have the same number of columns.", method, method));

    for (int i = 0; i < expected.size(); i++) {
      Change.ColumnValue expectedColumnValue = expected.get(i);
      Change.ColumnValue actualColumnValue = actual.get(i);

      assertEquals(expectedColumnValue.columnName(), actualColumnValue.columnName(), String.format("actual.%s().schemas(%d).%s() does not match", method, i, "columnName"));

      KafkaAssert.assertSchema(expectedColumnValue.schema(), actualColumnValue.schema(), String.format("actual.%s().schemas(%s).%s() does not match.", method, expectedColumnValue.columnName(), "schema"));

      if (Schema.Type.STRUCT == expectedColumnValue.schema().type()) {
        assertTrue(expectedColumnValue.value() instanceof Struct, String.format("expectedColumnValue(%s) should be a Struct.", expectedColumnValue.columnName()));
        assertTrue(actualColumnValue.value() instanceof Struct, String.format("actualColumnValue(%s) should be a Struct.", actualColumnValue.columnName()));
        assertStruct((Struct) expectedColumnValue.value(), (Struct) expectedColumnValue.value(), String.format("%s does not match", expectedColumnValue.columnName()));
      } else if (expectedColumnValue.value() instanceof byte[]) {
        byte[] expectedByteArray = (byte[]) expectedColumnValue.value();
        assertTrue(actualColumnValue.value() instanceof byte[], String.format("actual.%s().schemas(%s).%s() should be a byte array.", method, expectedColumnValue.columnName(), "value"));
        byte[] actualByteArray = (byte[]) actualColumnValue.value();
        assertArrayEquals(expectedByteArray, actualByteArray, String.format("actual.%s().schemas(%s).%s() does not match", method, expectedColumnValue.columnName(), "value"));
      } else {
        assertEquals(expectedColumnValue.value(), actualColumnValue.value(), String.format("actual.%s().columns(%s).%s() does not match", method, expectedColumnValue.columnName(), "value"));
      }
    }
  }

  public static void assertChange(Change expected, Change actual) {
    assertNotNull(expected, "expected should not be null");
    assertNotNull(actual, "actual should not be null");

    assertEquals(expected.databaseName(), actual.databaseName(), "databaseName does not match.");
    assertEquals(expected.schemaName(), actual.schemaName(), "schemaName does not match.");
    assertEquals(expected.tableName(), actual.tableName(), "tableName does not match.");
    assertEquals(expected.changeType(), actual.changeType(), "changeType does not match.");
    assertEquals(expected.timestamp(), actual.timestamp(), "timestamp does not match.");

    assertNotNull(expected.metadata(), "expected.metadata() cannot be null.");
    assertNotNull(actual.metadata(), "expected.metadata() cannot be null.");
    assertMap(expected.metadata(), actual.metadata(), "metadata");
    assertMap(expected.sourceOffset(), actual.sourceOffset(), "sourceOffset");
    assertMap(expected.sourcePartition(), actual.sourcePartition(), "sourcePartition");

    assertColumns(expected.keyColumns(), actual.keyColumns(), "keyColumns");
    assertColumns(expected.valueColumns(), actual.valueColumns(), "valueColumns");
  }

  public static void assertTableMetadata(TableMetadataProvider.TableMetadata expected, TableMetadataProvider.TableMetadata actual) {
    assertTableMetadata(expected, actual, null);
  }

  public static void assertTableMetadata(TableMetadataProvider.TableMetadata expected, TableMetadataProvider.TableMetadata actual, String message) {
    String prefix = Strings.isNullOrEmpty(message) ? "" : message + ": ";
    assertNotNull(expected, prefix + "expected should not be null.");
    assertNotNull(actual, prefix + "actual should not be null.");

    assertNotNull(expected.columnSchemas(), prefix + "expected.columnSchemas() should not be null.");
    assertNotNull(actual.columnSchemas(), prefix + "actual.columnSchemas() should not be null.");

    assertEquals(expected.databaseName(), actual.databaseName(), prefix + "databaseName does not match.");
    assertEquals(expected.schemaName(), actual.schemaName(), prefix + "schemaName does not match.");
    assertEquals(expected.tableName(), actual.tableName(), prefix + "tableName does not match.");
    assertEquals(expected.keyColumns(), actual.keyColumns(), prefix + "keyColumns() does not match.");
  }
}
