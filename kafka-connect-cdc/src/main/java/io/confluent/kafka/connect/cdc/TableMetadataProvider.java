package io.confluent.kafka.connect.cdc;

import org.apache.kafka.connect.data.Schema;

import java.sql.SQLException;
import java.util.Map;
import java.util.Set;

public interface TableMetadataProvider {
  TableMetadata tableMetadata(ChangeKey changeKey) throws SQLException;

  Map<String, Object> startOffset(ChangeKey changeKey) throws SQLException;

  void cacheOffset(ChangeKey changeKey, Map<String, Object> offset);

  interface TableMetadata {
    String databaseName();

    String schemaName();

    String tableName();

    Set<String> keyColumns();

    Map<String, Schema> columnSchemas();
  }

}
