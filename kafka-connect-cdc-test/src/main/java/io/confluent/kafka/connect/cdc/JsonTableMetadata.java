package io.confluent.kafka.connect.cdc;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.kafka.connect.data.Schema;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.Set;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY, getterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class JsonTableMetadata implements TableMetadataProvider.TableMetadata, NamedTest {
  @JsonIgnore
  String name;
  String databaseName;
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
  public String databaseName() {
    return this.databaseName;
  }

  public void databaseName(String databaseName) {
    this.databaseName = databaseName;
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

  public static JsonTableMetadata of(TableMetadataProvider.TableMetadata tableMetadata) {
    JsonTableMetadata jsonTableMetadata = new JsonTableMetadata();
    jsonTableMetadata.databaseName = tableMetadata.databaseName();
    jsonTableMetadata.schemaName = tableMetadata.schemaName();
    jsonTableMetadata.tableName = tableMetadata.tableName();
    jsonTableMetadata.columnSchemas = tableMetadata.columnSchemas();
    jsonTableMetadata.keyColumns = tableMetadata.keyColumns();
    return jsonTableMetadata;
  }

  public static void write(File file, JsonTableMetadata change) throws IOException {
    try (OutputStream outputStream = new FileOutputStream(file)) {
      ObjectMapperFactory.instance.writeValue(outputStream, change);
    }
  }

  public static void write(OutputStream outputStream, JsonTableMetadata change) throws IOException {
    ObjectMapperFactory.instance.writeValue(outputStream, change);
  }

  public static JsonTableMetadata read(InputStream inputStream) throws IOException {
    return ObjectMapperFactory.instance.readValue(inputStream, JsonTableMetadata.class);
  }

  @Override
  public void name(String name) {
    this.name = name;
  }

  @Override
  public String name() {
    return this.name;
  }
}
