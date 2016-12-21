package io.confluent.kafka.connect.cdc.mssql;

import io.confluent.kafka.connect.cdc.CachingTableMetadataProvider;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

class MsSqlTableMetadataProvider extends CachingTableMetadataProvider {
  private static Logger log = LoggerFactory.getLogger(MsSqlTableMetadataProvider.class);
  final String PRIMARY_KEY_SQL =
      "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE " +
          "OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA+'.'+CONSTRAINT_NAME), 'IsPrimaryKey') = 1 AND " +
          "CONSTRAINT_SCHEMA = ? AND TABLE_NAME = ?";
  final String COLUMN_DEFINITION_SQL =
      "SELECT column_name, iif(is_nullable='YES', 1, 0) AS is_optional, data_type, " +
          "numeric_scale FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? " +
          "ORDER BY ORDINAL_POSITION";

  public MsSqlTableMetadataProvider(MsSqlSourceConnectorConfig config) {
    super(config);
  }

  @Override
  protected TableMetadata fetchTableMetadata(String databaseName, String schemaName, String tableName) throws SQLException {
    try (Connection connection = openConnection()) {
      if (log.isDebugEnabled()) {
        log.debug("Querying for primary keys for [{}].[{}].[{}]", databaseName, schemaName, tableName);
      }

      Set<String> keyColumns = new LinkedHashSet<>();
      try (PreparedStatement primaryKeyStatement = connection.prepareStatement(PRIMARY_KEY_SQL)) {
        primaryKeyStatement.setString(1, schemaName);
        primaryKeyStatement.setString(2, tableName);
        try (ResultSet resultSet = primaryKeyStatement.executeQuery()) {
          while (resultSet.next()) {
            keyColumns.add(resultSet.getString(1));
          }
        }
      }

      if (log.isDebugEnabled()) {
        log.debug("Querying for schema for [{}].[{}].[{}]", databaseName, schemaName, tableName);
      }

      Map<String, Schema> columnSchemas = new LinkedHashMap<>();
      try (PreparedStatement columnDefinitionStatement = connection.prepareStatement(COLUMN_DEFINITION_SQL)) {
        columnDefinitionStatement.setString(1, schemaName);
        columnDefinitionStatement.setString(2, tableName);
        try (ResultSet resultSet = columnDefinitionStatement.executeQuery()) {
          while (resultSet.next()) {
            String columnName = resultSet.getString(1);
            Schema schema = generateSchema(resultSet, schemaName, tableName);
            columnSchemas.put(columnName, schema);
          }
        }
      }
      return new MsSqlTableMetadata(databaseName, schemaName, tableName, keyColumns, columnSchemas);
    }
  }

  Schema generateSchema(ResultSet resultSet, final String schemaName, final String tableName) throws SQLException {
    boolean optional = resultSet.getBoolean(2);
    String dataType = resultSet.getString(3);
    int scale = resultSet.getInt(4);
    SchemaBuilder builder;

    switch (dataType) {
      case "bigint":
        builder = SchemaBuilder.int64();
        break;
      case "varchar":
      case "text":
      case "nvarchar":
      case "ntext":
        builder = SchemaBuilder.string();
        break;
      case "decimal":
        builder = Decimal.builder(scale);
        break;
      default:
        throw new DataException(
            String.format("Could not process type for table %s.%s (dataType = '%s', optional = %s, scale = %d)",
                schemaName, tableName, dataType, optional, scale
            )
        );
    }

    if (optional) {
      builder.optional();
    }

    return builder.build();
  }

  static class MsSqlTableMetadata implements TableMetadata {
    final String databaseName;
    final String schemaName;
    final String tableName;
    final Set<String> keyColumns;
    final Map<String, Schema> columnSchemas;

    MsSqlTableMetadata(String databaseName, String schemaName, String tableName, Set<String> keyColumns, Map<String, Schema> columnSchemas) {
      this.databaseName = databaseName;
      this.schemaName = schemaName;
      this.tableName = tableName;
      this.keyColumns = keyColumns;
      this.columnSchemas = columnSchemas;
    }

    @Override
    public String databaseName() {
      return this.databaseName;
    }

    @Override
    public String schemaName() {
      return this.schemaName;
    }

    @Override
    public String tableName() {
      return this.tableName;
    }

    @Override
    public Set<String> keyColumns() {
      return this.keyColumns;
    }

    @Override
    public Map<String, Schema> columnSchemas() {
      return this.columnSchemas;
    }
  }
}
