package io.confluent.kafka.connect.cdc;

import org.apache.kafka.connect.data.Schema;

import java.util.Map;
import java.util.Set;

public interface TableMetadataProvider {
  TableMetadata tableMetadata(String schemaName, String tableName);

  interface TableMetadata {
    String schemaName();

    String tableName();

    Set<String> keyColumns();

    Map<String, Schema> columnSchemas();
  }

}
