/**
 * Copyright © 2017 Jeremy Custenborder (jcustenborder@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jcustenborder.kafka.connect.cdc;

import com.google.common.collect.ImmutableMap;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SchemaGeneratorTest {
  static final String EXPECTED_SOURCE_DATABASE_NAME = "TESTDATABASE";
  static final String EXPECTED_SOURCE_SCHEMA_NAME = "SCOTT";
  static final String EXPECTED_SOURCE_TABLE_NAME = "TEST_TABLE";


  SchemaGenerator schemaGenerator;
  Change change;


  Change mockChange() {
    Change change = mock(Change.class);
    when(change.databaseName()).thenReturn(EXPECTED_SOURCE_DATABASE_NAME);
    when(change.schemaName()).thenReturn(EXPECTED_SOURCE_SCHEMA_NAME);
    when(change.tableName()).thenReturn(EXPECTED_SOURCE_TABLE_NAME);
    when(change.changeType()).thenReturn(Change.ChangeType.INSERT);

    List<Change.ColumnValue> valueColumns = new ArrayList<>();
    TestData.addColumnValue(valueColumns, "FIRST_NAME", SchemaBuilder.string().parameter(Change.ColumnValue.COLUMN_NAME, "FIRST_NAME").build(), "John");
    TestData.addColumnValue(valueColumns, "LAST_NAME", SchemaBuilder.string().parameter(Change.ColumnValue.COLUMN_NAME, "LAST_NAME").build(), "Doe");
    TestData.addColumnValue(valueColumns, "EMAIL", SchemaBuilder.string().optional().parameter(Change.ColumnValue.COLUMN_NAME, "EMAIL").build(), "john.doe@example.com");

    when(change.valueColumns()).thenReturn(valueColumns);

    List<Change.ColumnValue> keyColumns = new ArrayList<>();
    TestData.addColumnValue(keyColumns, "EMAIL", SchemaBuilder.string().optional().parameter(Change.ColumnValue.COLUMN_NAME, "EMAIL").build(), "john.doe@example.com");
    when(change.keyColumns()).thenReturn(keyColumns);

    return change;
  }

  @BeforeEach
  public void before() {
    CDCSourceConnectorConfig config = new CDCSourceConnectorConfig(CDCSourceConnectorConfig.config(), CDCSourceConnectorConfigTest.settings());
    this.schemaGenerator = new SchemaGenerator(config);
    this.change = mockChange();
  }

  @Test
  public void values() {
    Map<String, String> settings = CDCSourceConnectorConfigTest.settings();
    settings.put(CDCSourceConnectorConfig.SCHEMA_CASE_FORMAT_INPUT_CONFIG, CDCSourceConnectorConfig.CaseFormat.LOWER_UNDERSCORE.name());
    settings.put(CDCSourceConnectorConfig.SCHEMA_CASE_FORMAT_DATABASE_NAMES_DOC, CDCSourceConnectorConfig.CaseFormat.LOWER.name());
    settings.put(CDCSourceConnectorConfig.SCHEMA_CASE_FORMAT_TABLE_NAMES_CONFIG, CDCSourceConnectorConfig.CaseFormat.UPPER_CAMEL.name());
    settings.put(CDCSourceConnectorConfig.SCHEMA_CASE_FORMAT_SCHEMA_NAMES_CONFIG, CDCSourceConnectorConfig.CaseFormat.LOWER.name());

    CDCSourceConnectorConfig config = new CDCSourceConnectorConfig(CDCSourceConnectorConfig.config(), settings);

    SchemaGenerator generator = new SchemaGenerator(config);

    Change c = mock(Change.class);
    when(c.databaseName()).thenReturn("TESTING");
    when(c.tableName()).thenReturn("user");
    when(c.schemaName()).thenReturn("dbo");

    Map<String, String> actual = generator.values(c, null);
    Map<String, String> expected = ImmutableMap.of(
        Constants.NAMESPACE_VARIABLE, "",
        Constants.DATABASE_NAME_VARIABLE, "testing",
        Constants.SCHEMA_NAME_VARIABLE, "dbo",
        Constants.TABLE_NAME_VARIABLE, "User"
    );

    assertEquals(expected, actual);
  }

  private void assertConvertCase(String input, String expected, CDCSourceConnectorConfig.CaseFormat inputCaseFormat, CDCSourceConnectorConfig.CaseFormat outputCaseFormat) {
    final String actual = SchemaGenerator.convertCase(input, inputCaseFormat, outputCaseFormat);
    assertEquals(expected, actual);
  }

  @Test
  public void convertCase_NONE_NONE() {
    assertConvertCase("TeStINg", "TeStINg", CDCSourceConnectorConfig.CaseFormat.NONE, CDCSourceConnectorConfig.CaseFormat.NONE);
  }

  @Test
  public void convertCase_UPPER_UNDERSCORE_UPPER_CAMEL() {
    assertConvertCase("USERS", "Users", CDCSourceConnectorConfig.CaseFormat.UPPER_UNDERSCORE, CDCSourceConnectorConfig.CaseFormat.UPPER_CAMEL);
    assertConvertCase("FIRST_NAME", "FirstName", CDCSourceConnectorConfig.CaseFormat.UPPER_UNDERSCORE, CDCSourceConnectorConfig.CaseFormat.UPPER_CAMEL);
  }

  @Test
  public void convertCase_UPPER_UNDERSCORE_UPPER_LOWER_CAMEL() {
    assertConvertCase("USERS", "users", CDCSourceConnectorConfig.CaseFormat.UPPER_UNDERSCORE, CDCSourceConnectorConfig.CaseFormat.LOWER_CAMEL);
    assertConvertCase("FIRST_NAME", "firstName", CDCSourceConnectorConfig.CaseFormat.UPPER_UNDERSCORE, CDCSourceConnectorConfig.CaseFormat.LOWER_CAMEL);
  }

  @Test
  public void namespace() {
    final String expectedNamespace = "com.example.data.testdatabase";
    final String actual = this.schemaGenerator.namespace(change);
    assertEquals(expectedNamespace, actual);
  }

  @Test
  public void valueSchemaName() {
    final String schemaName = "com.example.data.testdatabase.TestTableValue";
    final String actual = this.schemaGenerator.valueSchemaName(change);
    assertEquals(schemaName, actual);
  }

  @Test
  public void keySchemaName() {
    final String schemaName = "com.example.data.testdatabase.TestTableKey";
    final String actual = this.schemaGenerator.keySchemaName(change);
    assertEquals(schemaName, actual);
  }

  @Test
  public void generateValueSchema() {
    List<String> fieldNames = new ArrayList<>();
    final Schema expected = SchemaBuilder.struct()
        .name("com.example.data.testdatabase.TestTableValue")
        .field(Constants.METADATA_FIELD, SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA))
        .field("firstName", SchemaBuilder.string().parameter(Change.ColumnValue.COLUMN_NAME, "FIRST_NAME").build())
        .field("lastName", SchemaBuilder.string().parameter(Change.ColumnValue.COLUMN_NAME, "LAST_NAME").build())
        .field("email", SchemaBuilder.string().optional().parameter(Change.ColumnValue.COLUMN_NAME, "EMAIL").build())
        .parameters(
            ImmutableMap.of(
                Change.DATABASE_NAME, this.change.databaseName(),
                Change.SCHEMA_NAME, this.change.schemaName(),
                Change.TABLE_NAME, this.change.tableName()
            )
        )
        .build();
    Schema actual = this.schemaGenerator.generateValueSchema(this.change, fieldNames);
    Assertions.assertSchema(expected, actual);
  }

  @Test
  public void generateKeySchema() {
    List<String> fieldNames = new ArrayList<>();
    final Schema expected = SchemaBuilder.struct()
        .name("com.example.data.testdatabase.TestTableKey")
        .field("email", SchemaBuilder.string().optional().parameter(Change.ColumnValue.COLUMN_NAME, "EMAIL").build())
        .parameters(
            ImmutableMap.of(
                Change.DATABASE_NAME, this.change.databaseName(),
                Change.SCHEMA_NAME, this.change.schemaName(),
                Change.TABLE_NAME, this.change.tableName()
            )
        )
        .build();

    Schema schema = this.schemaGenerator.generateKeySchema(this.change, fieldNames);
    Assertions.assertSchema(expected, schema);
  }

}
