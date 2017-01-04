package io.confluent.kafka.connect.cdc.mssql;

import com.google.common.collect.ImmutableSet;
import io.codearte.jfairy.Fairy;
import io.codearte.jfairy.producer.person.Person;
import io.confluent.kafka.connect.cdc.Change;
import io.confluent.kafka.connect.cdc.TableMetadataProvider;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.junit.jupiter.api.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MsSqlChangeTests {

  @Test
  public void insert() throws SQLException {

    Fairy fairy = Fairy.create();
    Person person = fairy.person();

    long EXPECTED_USERID = 12345L;
    String EXPECTED_IP = fairy.networkProducer().ipAddress();

    Time time = mock(Time.class);
    when(time.milliseconds()).thenReturn(1482263155123L);

    TableMetadataProvider.TableMetadata tableMetadata = mock(TableMetadataProvider.TableMetadata.class);
    when(tableMetadata.schemaName()).thenReturn("dbo");
    when(tableMetadata.tableName()).thenReturn("users");
    when(tableMetadata.keyColumns()).thenReturn(ImmutableSet.of("user_id"));

    Map<String, Schema> columnSchemas = new LinkedHashMap<>();
    columnSchemas.put("user_id", Schema.INT64_SCHEMA);
    columnSchemas.put("first_name", Schema.OPTIONAL_STRING_SCHEMA);
    columnSchemas.put("last_name", Schema.OPTIONAL_STRING_SCHEMA);
    columnSchemas.put("email", Schema.OPTIONAL_STRING_SCHEMA);
    columnSchemas.put("gender", Schema.OPTIONAL_STRING_SCHEMA);
    columnSchemas.put("ip_address", Schema.OPTIONAL_STRING_SCHEMA);
    columnSchemas.put("company_name", Schema.OPTIONAL_STRING_SCHEMA);
    columnSchemas.put("country_code", Schema.OPTIONAL_STRING_SCHEMA);
    columnSchemas.put("latitude", Decimal.builder(6).optional().build());
    columnSchemas.put("longitude", Decimal.builder(6).optional().build());
    columnSchemas.put("account_balance", Decimal.builder(6).optional().build());
    columnSchemas.put("username", Schema.OPTIONAL_STRING_SCHEMA);
    when(tableMetadata.columnSchemas()).thenReturn(columnSchemas);


    MsSqlChange.Builder builder = MsSqlChange.builder();
    ResultSet resultSet = mock(ResultSet.class);
    when(resultSet.getLong("__metadata_sys_change_version")).thenReturn(1L);
    when(resultSet.getLong("__metadata_sys_change_creation_version")).thenReturn(1L);
    when(resultSet.getString("__metadata_sys_change_operation")).thenReturn("I");
    when(resultSet.getObject("user_id")).thenReturn(EXPECTED_USERID);
    when(resultSet.getObject("first_name")).thenReturn(person.getFirstName());
    when(resultSet.getObject("last_name")).thenReturn(person.getLastName());
    when(resultSet.getObject("email")).thenReturn(person.getCompanyEmail());
    when(resultSet.getObject("gender")).thenReturn(person.getSex().name());
    when(resultSet.getObject("ip_address")).thenReturn(EXPECTED_IP);
    when(resultSet.getObject("company_name")).thenReturn(person.getCompany().getName());
    when(resultSet.getObject("country_code")).thenReturn("US");
    when(resultSet.getObject("username")).thenReturn(person.getUsername());
    when(resultSet.getObject("latitude")).thenReturn(null);
    when(resultSet.getObject("longitude")).thenReturn(null);
    when(resultSet.getObject("account_balance")).thenReturn(null);
    Change actual = builder.build(tableMetadata, resultSet, time);
  }


}
