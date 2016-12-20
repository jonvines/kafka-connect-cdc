package io.confluent.kafka.connect.cdc;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.kafka.connect.data.Schema;

import java.util.Map;
import java.util.Set;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY, getterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class JsonTableMetadata implements TableMetadataProvider.TableMetadata {
  String schemaName;
  String tableName;
  Set<String> keyColumns;
  Map<String, Schema> columnSchemas;

  public JsonTableMetadata() {
  }

  public JsonTableMetadata(String schemaName, String tableName, Set<String> keyColumns, Map<String, Schema> columnSchemas) {
    this.schemaName = schemaName;
    this.tableName = tableName;
    this.keyColumns = keyColumns;
    this.columnSchemas = columnSchemas;
  }

  @Override
  public String schemaName() {
    return this.schemaName;
  }

  public void schemaName(String value) {
    this.schemaName = value;
  }

  @Override
  public String tableName() {
    return this.tableName;
  }

  public void tableName(String value) {
    this.tableName = value;
  }

  @Override
  public Set<String> keyColumns() {
    return this.keyColumns;
  }

  public void keyColumns(Set<String> value) {
    this.keyColumns = value;
  }

  @Override
  public Map<String, Schema> columnSchemas() {
    return this.columnSchemas;
  }

  public void keyColumns(Map<String, Schema> value) {
    this.columnSchemas = value;
  }
}
