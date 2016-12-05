package io.confluent.kafka.connect.cdc;

import org.apache.kafka.connect.data.Schema;

public interface ColumnValue {
  /**
   * Name of the column.
   * @return
   */
  String columnName();

  /**
   * Schema for the data.
   * @return
   */
  Schema schema();

  /**
   * Value for the data.
   * @return
   */
  Object value();
}
