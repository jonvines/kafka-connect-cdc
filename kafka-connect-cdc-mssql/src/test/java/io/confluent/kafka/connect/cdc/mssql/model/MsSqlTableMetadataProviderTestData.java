package io.confluent.kafka.connect.cdc.mssql.model;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import io.confluent.kafka.connect.cdc.NamedTest;
import io.confluent.kafka.connect.cdc.ObjectMapperFactory;
import io.confluent.kafka.connect.cdc.TableMetadataProvider;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY, getterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class MsSqlTableMetadataProviderTestData implements NamedTest {
  String databaseName;
  String schemaName;
  String tableName;
  TableMetadataProvider.TableMetadata expected;

  public static void write(File file, MsSqlTableMetadataProviderTestData testData) throws IOException {
    try (OutputStream outputStream = new FileOutputStream(file)) {
      ObjectMapperFactory.instance.writeValue(outputStream, testData);
    }
  }

  public static void write(OutputStream outputStream, MsSqlTableMetadataProviderTestData testData) throws IOException {
    ObjectMapperFactory.instance.writeValue(outputStream, testData);
  }

  public static MsSqlTableMetadataProviderTestData read(InputStream inputStream) throws IOException {
    return ObjectMapperFactory.instance.readValue(inputStream, MsSqlTableMetadataProviderTestData.class);
  }

  @Override
  public String name() {
    return String.format("%s.%s", this.schemaName, this.tableName);
  }

  @Override
  public void name(String value) {

  }

  public String databaseName() {
    return this.databaseName;
  }

  public void databaseName(String databaseName) {
    this.databaseName = databaseName;
  }

  public String schemaName() {
    return this.schemaName;
  }

  public void schemaName(String value) {
    this.schemaName = value;
  }

  public String tableName() {
    return this.tableName;
  }

  public void tableName(String value) {
    this.tableName = value;
  }

  public TableMetadataProvider.TableMetadata expected() {
    return this.expected;
  }

  public void expected(TableMetadataProvider.TableMetadata value) {
    this.expected = value;
  }
}
