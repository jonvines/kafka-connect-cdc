package io.confluent.kafka.connect.cdc.mssql;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.connect.cdc.Change;
import io.confluent.kafka.connect.cdc.TableMetadataProvider;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Schema;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

class MsSqlChange implements Change {
  @Override
  public Map<String, String> metadata() {
    return this.metadata;
  }

  @Override
  public Map<String, Object> sourcePartition() {
    return this.sourcePartition;
  }

  @Override
  public Map<String, Object> sourceOffset() {
    return this.sourceOffset;
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
  public List<ColumnValue> keyColumns() {
    return this.keyColumns;
  }

  @Override
  public List<ColumnValue> valueColumns() {
    return this.valueColumns;
  }

  @Override
  public ChangeType changeType() {
    return this.changeType;
  }

  @Override
  public long timestamp() {
    return this.timestamp;
  }

  public static Builder builder() {
    return new Builder();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(MsSqlChange.class)
        .omitNullValues()
        .add("databaseName", this.databaseName)
        .add("schemaName", this.schemaName)
        .add("tableName", this.tableName)
        .add("changeType", this.changeType)
        .add("timestamp", this.timestamp)
        .add("metadata", this.metadata)
        .add("sourcePartition", this.sourcePartition)
        .add("sourceOffset", this.sourceOffset)
        .add("keyColumns", this.keyColumns)
        .add("valueColumns", this.valueColumns)
        .toString();
  }

  Map<String, String> metadata;
  Map<String, Object> sourcePartition;
  Map<String, Object> sourceOffset;
  String databaseName;
  String schemaName;
  String tableName;
  long timestamp;
  ChangeType changeType;
  List<ColumnValue> keyColumns;
  List<ColumnValue> valueColumns;

  static class MsSqlColumnValue implements ColumnValue {

    final String columnName;
    final Schema schema;
    final Object value;

    MsSqlColumnValue(String columnName, Schema schema, Object value) {
      this.columnName = columnName;
      this.schema = schema;
      this.value = value;
    }


    @Override
    public String columnName() {
      return this.columnName;
    }

    @Override
    public Schema schema() {
      return this.schema;
    }

    @Override
    public Object value() {
      return this.value;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(MsSqlColumnValue.class)
          .omitNullValues()
          .add("columnName", this.columnName)
          .add("schema", this.schema)
          .add("value", this.value)
          .toString();
    }
  }

  static class Builder {
    static Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));

    public MsSqlChange build(TableMetadataProvider.TableMetadata tableMetadata, ResultSet resultSet, Time time) throws SQLException {
      MsSqlChange change = new MsSqlChange();
      change.timestamp = time.milliseconds();
      change.databaseName = tableMetadata.databaseName();
      change.schemaName = tableMetadata.schemaName();
      change.tableName = tableMetadata.tableName();

      long sys_change_version = resultSet.getLong("__metadata_sys_change_version");
      long sys_change_creation_version = resultSet.getLong("__metadata_sys_change_creation_version");
      String changeOperation = resultSet.getString("__metadata_sys_change_operation");

      change.metadata = ImmutableMap.of(
          "sys_change_operation", changeOperation,
          "sys_change_creation_version", String.valueOf(sys_change_creation_version),
          "sys_change_version", String.valueOf(sys_change_version)
      );

      switch (changeOperation) {
        case "I":
          change.changeType = ChangeType.INSERT;
          break;
        case "U":
          change.changeType = ChangeType.UPDATE;
          break;
        case "D":
          change.changeType = ChangeType.UPDATE;
          break;
        default:
          throw new UnsupportedOperationException(
              String.format("Unsupported sys_change_operation of '%s'", changeOperation)
          );
      }

      change.keyColumns = new ArrayList<>(tableMetadata.keyColumns().size());
      change.valueColumns = new ArrayList<>(tableMetadata.columnSchemas().size());

      for (Map.Entry<String, Schema> kvp : tableMetadata.columnSchemas().entrySet()) {
        String columnName = kvp.getKey();
        Schema schema = kvp.getValue();
        Object value;
        if (Schema.Type.INT8 == schema.type()) {
          // Really lame Microsoft. A tiny int is stored as a single byte with a value of 0-255.
          // Explain how this should be returned as a short?
          value = resultSet.getByte(columnName);
        } else if (Schema.Type.INT32 == schema.type() &&
            Date.LOGICAL_NAME.equals(schema.name())) {
          value = new java.util.Date(
              resultSet.getDate(columnName, calendar).getTime()
          );
        } else if (Schema.Type.INT32 == schema.type() &&
            org.apache.kafka.connect.data.Time.LOGICAL_NAME.equals(schema.name())) {
          value = new java.util.Date(
              resultSet.getTime(columnName, calendar).getTime()
          );
        } else {
          value = resultSet.getObject(columnName);
        }


        MsSqlColumnValue columnValue = new MsSqlColumnValue(columnName, schema, value);
        change.valueColumns.add(columnValue);
        if (tableMetadata.keyColumns().contains(columnName)) {
          change.keyColumns.add(columnValue);
        }
      }

      //Revisit this. Pretty lame.
      if (ChangeType.DELETE == change.changeType) {
        change.valueColumns.clear();
      }

      return change;
    }
  }
}
