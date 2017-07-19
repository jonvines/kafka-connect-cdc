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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.apache.kafka.connect.data.Schema;

import java.util.List;
import java.util.Map;

public interface Change {
  /**
   * The name of the column before it was changed.
   */
  public static final String DATABASE_NAME = "com.github.jcustenborder.kafka.connect.cdc.change.database.name";
  public static final String SCHEMA_NAME = "com.github.jcustenborder.kafka.connect.cdc.change.schema.name";
  public static final String TABLE_NAME = "com.github.jcustenborder.kafka.connect.cdc.change.table.name";

  static Map<String, Object> sourcePartition(Change change) {
    Preconditions.checkNotNull(change, "change cannot be null.");
    return ImmutableMap.of(
        Constants.DATABASE_NAME_VARIABLE, change.databaseName(),
        Constants.SCHEMA_NAME_VARIABLE, change.schemaName(),
        Constants.TABLE_NAME_VARIABLE, change.tableName()
    );
  }

  static Map<String, Object> sourcePartition(ChangeKey change) {
    Preconditions.checkNotNull(change, "change cannot be null.");
    return ImmutableMap.of(
        Constants.DATABASE_NAME_VARIABLE, change.databaseName,
        Constants.SCHEMA_NAME_VARIABLE, change.schemaName,
        Constants.TABLE_NAME_VARIABLE, change.tableName
    );
  }

  static Map<String, Object> sourcePartition(String databaseName, String schemaName, String tableName) {
    Preconditions.checkNotNull(databaseName, "databaseName cannot be null.");
    Preconditions.checkNotNull(schemaName, "schemaName cannot be null.");
    Preconditions.checkNotNull(tableName, "tableName cannot be null.");
    return ImmutableMap.of(
        Constants.DATABASE_NAME_VARIABLE, databaseName,
        Constants.SCHEMA_NAME_VARIABLE, schemaName,
        Constants.TABLE_NAME_VARIABLE, tableName
    );
  }

  /**
   * Metadata for the change.
   *
   * @return Metadata for the change.
   */
  Map<String, String> metadata();

  /**
   * Source partition for change.
   *
   * @return Source partition for change.
   */
  Map<String, Object> sourcePartition();

  /**
   * Source offset for the change.
   *
   * @return Source offset for the change.
   */
  Map<String, Object> sourceOffset();

  /**
   * Name of the database where the change originated from.
   *
   * @return Name of the database where the change originated from.
   */
  String databaseName();

  /**
   * Schema where the change originated from.
   *
   * @return Schema where the change originated from.
   */
  String schemaName();

  /**
   * Table that was changed.
   *
   * @return Table that was changed.
   */
  String tableName();

  /**
   * The columns with data for the key of the record.
   *
   * @return The columns with data for the key of the record.
   */
  List<ColumnValue> keyColumns();

  /**
   * The columns with data for the value of the record.
   *
   * @return The columns with data for the value of the record.
   */
  List<ColumnValue> valueColumns();

  /**
   * Type of change
   *
   * @return Type of change
   */
  ChangeType changeType();

  /**
   * Timestamp of when the transaction occurred.
   *
   * @return Timestamp of when the transaction occurred.
   */
  long timestamp();

  /**
   * Type of change
   */
  enum ChangeType {
    /**
     * Update of a row
     */
    UPDATE,
    /**
     * Insert of new data
     */
    INSERT,
    /**
     * Delete
     */
    DELETE
  }

  interface ColumnValue {

    /**
     * The name of the column before it was changed.
     */
    public static final String COLUMN_NAME = "com.github.jcustenborder.kafka.connect.cdc.change.column.name";

    /**
     * Name of the column.
     *
     * @return Name of the column.
     */
    String columnName();

    /**
     * Schema for the data.
     *
     * @return Schema for the data.
     */
    Schema schema();

    /**
     * Value for the data.
     *
     * @return Value for the data.
     */
    Object value();
  }
}
