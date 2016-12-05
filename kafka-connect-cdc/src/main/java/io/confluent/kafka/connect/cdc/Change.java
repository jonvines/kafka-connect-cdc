package io.confluent.kafka.connect.cdc;

import java.util.List;
import java.util.Map;

public interface Change {
  /**
   *
   * @return
   */
  Map<String, ?> sourcePartition();

  /**
   *
   * @return
   */
  Map<String, ?> sourceOffset();

  /**
   * Schema where the change originated from.
   * @return
   */
  String sourceDatabaseName();

  /**
   * Table that was changed.
   * @return
   */
  String tableName();

  /**
   * The columns with data for the key of the record.
   * @return
   */
  List<ColumnValue> keyColumns();

  /**
   * The columns with data for the value of the record.
   * @return
   */
  List<ColumnValue> valueColumns();

  /**
   * Type of change
   * @return
   */
  ChangeType changeType();

  /**
   * Timestamp of when the transaction occurred.
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
    INSERT
  }
}
