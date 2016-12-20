package io.confluent.kafka.connect.cdc;


import com.google.common.base.Strings;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import org.apache.kafka.connect.data.Schema;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class ChangeAssertions {

  public static void assertSchema(final Schema expected, final Schema actual) {
    assertSchema(expected, actual, null);
  }

  public static void assertSchema(final Schema expected, final Schema actual, String message) {
    String prefix = Strings.isNullOrEmpty(message) ? "" : message + ": ";
    assertNotNull(expected, prefix + "expected schema should not be null.");
    assertNotNull(actual, prefix + "actual schema should not be null.");
    assertEquals(expected.name(), actual.name(), prefix + "schema.name() should match.");
    assertEquals(expected.type(), actual.type(), prefix + "schema.type() should match.");
    assertEquals(expected.defaultValue(), actual.defaultValue(), prefix + "schema.defaultValue() should match.");
    assertEquals(expected.isOptional(), actual.isOptional(), prefix + "schema.isOptional() should match.");
    assertEquals(expected.doc(), actual.doc(), prefix + "schema.doc() should match.");
    assertEquals(expected.version(), actual.version(), prefix + "schema.version() should match.");
    assertMap(expected.parameters(), actual.parameters(), prefix + "schema.parameters() should match.");
    switch (expected.type()) {
      case ARRAY:
        assertSchema(expected.valueSchema(), actual.valueSchema(), message + "valueSchema does not match.");
        break;
      case MAP:
        assertSchema(expected.keySchema(), actual.keySchema(), message + "keySchema does not match.");
        assertSchema(expected.valueSchema(), actual.valueSchema(), message + "valueSchema does not match.");
        break;
    }
  }

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
    assertSchema(expected.schema(), actual.schema(), prefix + "schema should match.");
  }


  static void assertColumns(List<Change.ColumnValue> expected, List<Change.ColumnValue> actual, String method) {
    assertNotNull(expected, String.format("expected.%s() cannot be null.", method));
    assertNotNull(actual, String.format("actual.%s() cannot be null.", method));

    assertEquals(expected.size(), actual.size(), String.format("expected.%s() and actual.%s() do not have the same number of columns.", method, method));

    for (int i = 0; i < expected.size(); i++) {
      Change.ColumnValue expectedColumnValue = expected.get(i);
      Change.ColumnValue actualColumnValue = actual.get(i);

      assertEquals(expectedColumnValue.columnName(), actualColumnValue.columnName(), String.format("actual.%s().get(%d).%s() does not match", method, i, "columnName"));

      assertSchema(expectedColumnValue.schema(), actualColumnValue.schema(), String.format("actual.%s().get(%d).%s() does not match.", method, i, "schema"));

//      assertEquals(expectedColumnValue.value(), actualColumnValue.value(), String.format("actual.%s().get(%d).%s() does not match", method, i, "value"));
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

  public static void assertTableMetadata(TableMetadataProvider.TableMetadata expected, TableMetadataProvider.TableMetadata actual) {
    assertTableMetadata(expected, actual, null);
  }

  public static void assertTableMetadata(TableMetadataProvider.TableMetadata expected, TableMetadataProvider.TableMetadata actual, String message) {
    String prefix = Strings.isNullOrEmpty(message) ? "" : message + ": ";
    assertNotNull(expected, prefix + "expected should not be null.");
    assertNotNull(actual, prefix + "actual should not be null.");
  }

}
