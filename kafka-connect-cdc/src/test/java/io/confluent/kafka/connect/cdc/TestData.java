package io.confluent.kafka.connect.cdc;

import org.apache.kafka.connect.data.Schema;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestData {
  public static final String EXPECTED_SOURCE_DATABASE_NAME = "TESTDATABASE";
  public static final String EXPECTED_SOURCE_TABLE_NAME = "TEST_TABLE";

  public static void addColumnValue(List<ColumnValue> columnValues, String columnName, Schema schema, Object value) {
    ColumnValue columnValue = mock(ColumnValue.class);
    when(columnValue.columnName()).thenReturn(columnName);
    when(columnValue.schema()).thenReturn(schema);
    when(columnValue.value()).thenReturn(value);
    columnValues.add(columnValue);
  }

  public static Change change() {
    Change change = mock(Change.class);
    when(change.sourceDatabaseName()).thenReturn(EXPECTED_SOURCE_DATABASE_NAME);
    when(change.tableName()).thenReturn(EXPECTED_SOURCE_TABLE_NAME);
    when(change.changeType()).thenReturn(Change.ChangeType.INSERT);

    List<ColumnValue> valueColumns = new ArrayList<>();
    addColumnValue(valueColumns, "first_name", Schema.OPTIONAL_STRING_SCHEMA, "John");
    addColumnValue(valueColumns, "last_name", Schema.OPTIONAL_STRING_SCHEMA, "Doe");
    addColumnValue(valueColumns, "email", Schema.OPTIONAL_STRING_SCHEMA, "john.doe@example.com");
    when(change.valueColumns()).thenReturn(valueColumns);

    List<ColumnValue> keyColumns = new ArrayList<>();
    addColumnValue(keyColumns, "email", Schema.OPTIONAL_STRING_SCHEMA, "john.doe@example.com");
    when(change.keyColumns()).thenReturn(keyColumns);

    return change;
  }


}
