package io.confluent.kafka.connect.cdc;

import org.apache.kafka.connect.data.Schema;

import java.sql.SQLException;
import java.util.Map;
import java.util.Set;

public interface TableMetadataProvider {
  TableMetadata tableMetadata(String databaseName, String schemaName, String tableName) throws SQLException;
  Map<String, Object> startOffset(String databaseName, String schemaName, String tableName) throws SQLException;


  interface TableMetadata {
    String databaseName();

    String schemaName();

    String tableName();

    Set<String> keyColumns();

    Map<String, Schema> columnSchemas();
  }

}
