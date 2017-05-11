/**
 * Copyright © 2017 Jeremy Custenborder (jcustenborder@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestData {
  public static final String EXPECTED_SOURCE_DATABASE_NAME = "TESTDATABASE";
  public static final String EXPECTED_SOURCE_SCHEMA_NAME = "SCOTT";
  public static final String EXPECTED_SOURCE_TABLE_NAME = "TEST_TABLE";

  public static void addColumnValue(List<Change.ColumnValue> columnValues, String columnName, Schema schema, Object value) {
    Change.ColumnValue columnValue = mock(Change.ColumnValue.class);
    when(columnValue.columnName()).thenReturn(columnName);
    when(columnValue.schema()).thenReturn(schema);
    when(columnValue.value()).thenReturn(value);
    columnValues.add(columnValue);
  }

  public static Change change() {
    Change change = mock(Change.class);
    when(change.databaseName()).thenReturn(EXPECTED_SOURCE_DATABASE_NAME);
    when(change.schemaName()).thenReturn(EXPECTED_SOURCE_SCHEMA_NAME);
    when(change.tableName()).thenReturn(EXPECTED_SOURCE_TABLE_NAME);
    when(change.changeType()).thenReturn(Change.ChangeType.INSERT);

    List<Change.ColumnValue> valueColumns = new ArrayList<>();
    addColumnValue(valueColumns, "first_name", SchemaBuilder.string().parameter(Change.ColumnValue.COLUMN_NAME, "first_name").build(), "John");
    addColumnValue(valueColumns, "last_name", SchemaBuilder.string().parameter(Change.ColumnValue.COLUMN_NAME, "last_name").build(), "Doe");
    addColumnValue(valueColumns, "email", SchemaBuilder.string().optional().parameter(Change.ColumnValue.COLUMN_NAME, "email").build(), "john.doe@example.com");
    when(change.valueColumns()).thenReturn(valueColumns);

    List<Change.ColumnValue> keyColumns = new ArrayList<>();
    addColumnValue(keyColumns, "email", SchemaBuilder.string().optional().parameter(Change.ColumnValue.COLUMN_NAME, "email").build(), "john.doe@example.com");
    when(change.keyColumns()).thenReturn(keyColumns);

    return change;
  }


}
