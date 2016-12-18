package io.confluent.kafka.connect.cdc;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SchemaGeneraterTest {
  static final String EXPECTED_SOURCE_DATABASE_NAME = "TESTDATABASE";
  static final String EXPECTED_SOURCE_TABLE_NAME = "TEST_TABLE";


  SchemaGenerater schemaGenerater;
  Change change;


  Change mockChange() {
    Change change = mock(Change.class);
    when(change.schemaName()).thenReturn(EXPECTED_SOURCE_DATABASE_NAME);
    when(change.tableName()).thenReturn(EXPECTED_SOURCE_TABLE_NAME);
    when(change.changeType()).thenReturn(Change.ChangeType.INSERT);

    List<ColumnValue> valueColumns = new ArrayList<>();
    TestData.addColumnValue(valueColumns, "first_name", Schema.OPTIONAL_STRING_SCHEMA, "John");
    TestData.addColumnValue(valueColumns, "last_name", Schema.OPTIONAL_STRING_SCHEMA, "Doe");
    TestData.addColumnValue(valueColumns, "email", Schema.OPTIONAL_STRING_SCHEMA, "john.doe@example.com");
    when(change.valueColumns()).thenReturn(valueColumns);

    List<ColumnValue> keyColumns = new ArrayList<>();
    TestData.addColumnValue(keyColumns, "email", Schema.OPTIONAL_STRING_SCHEMA, "john.doe@example.com");
    when(change.keyColumns()).thenReturn(keyColumns);

    return change;
  }

  @Before
  public void before() {
    CDCSourceConnectorConfig config = new CDCSourceConnectorConfig(CDCSourceConnectorConfig.config(), CDCSourceConnectorConfigTest.settings());
    this.schemaGenerater = new SchemaGenerater(config);
    this.change = mockChange();
  }

  @Test
  public void namespace() {
    final String expectedNamespace = "com.example.cdc.testdatabase";
    final String actual = this.schemaGenerater.namespace(change);
    assertEquals(expectedNamespace, actual);
  }

  @Test
  public void valueSchemaName() {
    final String schemaName = "com.example.cdc.testdatabase.TestTableValue";
    final String actual = this.schemaGenerater.valueSchemaName(change);
    assertEquals(schemaName, actual);
  }

  @Test
  public void keySchemaName() {
    final String schemaName = "com.example.cdc.testdatabase.TestTableKey";
    final String actual = this.schemaGenerater.keySchemaName(change);
    assertEquals(schemaName, actual);
  }

  @Test
  public void generateValueSchema() {
    List<String> fieldNames = new ArrayList<>();
    Schema schema = this.schemaGenerater.generateValueSchema(this.change, fieldNames);
    assertNotNull("schema should not be null.", schema);
    assertEquals("schema should be a struct.", Schema.Type.STRUCT, schema.type());
    assertEquals("name does not match", "com.example.cdc.testdatabase.TestTableValue", schema.name());

    List<Field> fields = schema.fields();
    assertNotNull("fields should not be null.", fields);
    assertEquals("fields count should be the same.", this.change.valueColumns().size() + 1, fields.size());

    Field field = fields.get(0);
    assertEquals("fields(0).name does not match.", "firstName", field.name());
    assertEquals("fields(0).schema does not match.", Schema.Type.STRING, field.schema().type());

    field = fields.get(1);
    assertEquals("fields(1).name does not match.", "lastName", field.name());
    assertEquals("fields(1).schema does not match.", Schema.Type.STRING, field.schema().type());

    field = fields.get(2);
    assertEquals("fields(2).name does not match.", "email", field.name());
    assertEquals("fields(2).schema does not match.", Schema.Type.STRING, field.schema().type());
  }

  @Test
  public void generateKeySchema() {
    List<String> fieldNames = new ArrayList<>();
    Schema schema = this.schemaGenerater.generateKeySchema(this.change, fieldNames);
    assertNotNull("schema should not be null.", schema);
    assertEquals("schema should be a struct.", Schema.Type.STRUCT, schema.type());
    assertEquals("name does not match", "com.example.cdc.testdatabase.TestTableKey", schema.name());

    List<Field> fields = schema.fields();
    assertNotNull("fields should not be null.", fields);
    assertEquals("fields count should be the same.", this.change.keyColumns().size(), fields.size());

    Field field = fields.get(0);
    assertEquals("fields(0).name does not match.", "email", field.name());
    assertEquals("fields(0).schema does not match.", Schema.Type.STRING, field.schema().type());
  }

}
