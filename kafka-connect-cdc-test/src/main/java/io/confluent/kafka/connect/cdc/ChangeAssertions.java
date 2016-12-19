package io.confluent.kafka.connect.cdc;


import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class ChangeAssertions {

  static void assertMap(Map<String, ?> expected, Map<String, ?> actual, String method) {
    assertNotNull(expected, String.format("expected.%s() cannot be null.", method));
    assertNotNull(actual, String.format("actual.%s() cannot be null.", method));
    MapDifference<String, ?> mapDifference = Maps.difference(expected, actual);
    assertTrue(mapDifference.areEqual(), new MapDifferenceSupplier(mapDifference, method));
  }

  static void assertColumns(List<Change.ColumnValue> expected, List<Change.ColumnValue> actual, String method) {
    assertNotNull(expected, String.format("expected.%s() cannot be null.", method));
    assertNotNull(actual, String.format("actual.%s() cannot be null.", method));

    assertEquals(expected.size(), actual.size(), String.format("expected.%s() and actual.%s() do not have the same number of columns.", method, method));

    for(int i=0;i<expected.size();i++) {
      Change.ColumnValue expectedColumnValue = expected.get(i);
      Change.ColumnValue actualColumnValue = actual.get(i);

      assertEquals(expectedColumnValue.columnName(), actualColumnValue.columnName(), String.format("actual.%s().get(%d).%s() does not match", method, i, "columnName"));
      assertEquals(expectedColumnValue.value(), actualColumnValue.value(), String.format("actual.%s().get(%d).%s() does not match", method, i, "value"));

      assertNotNull(expectedColumnValue.schema(), String.format("expected.%s().schema() should not be null.", method));
      assertNotNull(actualColumnValue.schema(), String.format("actual.%s().schema() should not be null.", method));
      assertEquals(expectedColumnValue.schema().name(), actualColumnValue.schema().name(), String.format("actual.%s().get(%d).%s() does not match", method, i, "schema().name"));
      assertEquals(expectedColumnValue.schema().type(), actualColumnValue.schema().type(), String.format("actual.%s().get(%d).%s() does not match", method, i, "schema().type"));
      assertEquals(expectedColumnValue.schema().isOptional(), actualColumnValue.schema().isOptional(), String.format("actual.%s().get(%d).%s() does not match", method, i, "schema().isOptional"));
    }

  }

  public static void assertChange(Change expected, Change actual) {
    assertNotNull(expected, "expected should not be null");
    assertNotNull(actual, "actual should not be null");

    assertEquals(expected.schemaName(), actual.schemaName(), "schemaName does not match.");
    assertEquals(expected.tableName(), actual.tableName(), "tableName does not match.");
    assertEquals(expected.changeType(), actual.changeType(), "changeType does not match.");
    assertEquals(expected.timestamp(), actual.timestamp(), "timestamp does not match.");

    assertMap(expected.metadata(), actual.metadata(), "metadata");
    assertMap(expected.sourceOffset(), actual.sourceOffset(), "sourceOffset");
    assertMap(expected.sourcePartition(), actual.sourcePartition(), "sourcePartition");

    assertColumns(expected.keyColumns(), actual.keyColumns(), "keyColumns");
    assertColumns(expected.valueColumns(), actual.valueColumns(), "valueColumns");
  }


}
