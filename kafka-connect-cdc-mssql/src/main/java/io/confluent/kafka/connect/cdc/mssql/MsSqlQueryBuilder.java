package io.confluent.kafka.connect.cdc.mssql;

import com.google.common.base.Preconditions;
import io.confluent.kafka.connect.cdc.TableMetadataProvider;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Set;

class MsSqlQueryBuilder {
  final Connection connection;

  MsSqlQueryBuilder(Connection connection) {
    this.connection = connection;
  }

  final String LIST_CHANGE_TRACKING_TABLES_SQL ="SELECT DB_NAME() as [databaseName], " +
      "SCHEMA_NAME(OBJECTPROPERTY(object_id, 'SchemaId')) as [schemaName], " +
      "OBJECT_NAME(object_id) as [tableName], " +
      "min_valid_version, " +
      "begin_version " +
      "FROM " +
      "[sys].[change_tracking_tables]";

  String joinCriteria(Set<String> keyColumns) {
    StringBuilder joinCriteria = new StringBuilder();
    for (String keyColumn : keyColumns) {
      if (joinCriteria.length() > 0) {
        joinCriteria.append(" AND ");
      }

      joinCriteria.append(
          String.format("[ct].[%s] = [u].[%s]", keyColumn, keyColumn)
      );
    }
    return joinCriteria.toString();
  }

  String changeTrackingStatementQuery(TableMetadataProvider.TableMetadata tableMetadata) {
    Preconditions.checkState(
        tableMetadata.keyColumns().size() > 0,
        "Table([%s].[%s]) must have at least one primary key column.",
        tableMetadata.schemaName(),
        tableMetadata.tableName()
    );
    String joinCriteria = joinCriteria(tableMetadata.keyColumns());
    final String SQL = String.format("SELECT " +
            "[ct].[sys_change_version] AS [__metadata_sys_change_version], " +
            "[ct].[sys_change_creation_version] AS [__metadata_sys_change_creation_version], " +
            "[ct].[sys_change_operation] AS [__metadata_sys_change_operation], " +
            "[u].* " +
            "FROM [%s].[%s] AS [u] " +
            "RIGHT OUTER JOIN " +
            "CHANGETABLE(CHANGES [%s].[%s], ?) AS [ct] " +
            "ON %s",
        tableMetadata.schemaName(),
        tableMetadata.tableName(),
        tableMetadata.schemaName(),
        tableMetadata.tableName(),
        joinCriteria
    );
    return SQL;
  }

  public PreparedStatement listChangeTrackingTablesStatement() throws SQLException {
    return this.connection.prepareStatement(LIST_CHANGE_TRACKING_TABLES_SQL);
  }

  public PreparedStatement changeTrackingStatement(TableMetadataProvider.TableMetadata tableMetadata) throws SQLException {
    final String SQL = changeTrackingStatementQuery(tableMetadata);
    return this.connection.prepareStatement(SQL);
  }
}