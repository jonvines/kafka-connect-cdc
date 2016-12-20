package io.confluent.kafka.connect.cdc;

import org.apache.kafka.connect.data.Schema;

import java.util.List;
import java.util.Map;

public interface Change {
  Map<String, String> metadata();

  /**
   * @return
   */
  Map<String, Object> sourcePartition();

  /**
   * @return
   */
  Map<String, Object> sourceOffset();

  /**
   * Name of the database where the change originated from.
   * @return
   */
  String databaseName();

  /**
   * Schema where the change originated from.
   *
   * @return
   */
  String schemaName();

  /**
   * Table that was changed.
   *
   * @return
   */
  String tableName();

  /**
   * The columns with data for the key of the record.
   *
   * @return
   */
  List<ColumnValue> keyColumns();

  /**
   * The columns with data for the value of the record.
   *
   * @return
   */
  List<ColumnValue> valueColumns();

  /**
   * Type of change
   *
   * @return
   */
  ChangeType changeType();

  /**
   * Timestamp of when the transaction occurred.
   *
   * @return
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
     * Name of the column.
     *
     * @return
     */
    String columnName();

    /**
     * Schema for the data.
     *
     * @return
     */
    Schema schema();

    /**
     * Value for the data.
     *
     * @return
     */
    Object value();
  }
}
