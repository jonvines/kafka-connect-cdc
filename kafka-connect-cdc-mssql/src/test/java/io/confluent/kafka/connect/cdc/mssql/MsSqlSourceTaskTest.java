package io.confluent.kafka.connect.cdc.mssql;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.Map;

public class MsSqlSourceTaskTest extends DockerTest {
  MsSqlSourceTask task;

  @BeforeEach
  public void before() {
    Map<String, String> settings = ImmutableMap.of(
        MsSqlSourceConnectorConfig.JDBC_URL_CONF, jdbcUrl(DATABASE_NAME),
        MsSqlSourceConnectorConfig.JDBC_USERNAME_CONF, USERNAME,
        MsSqlSourceConnectorConfig.JDBC_PASSWORD_CONF, PASSWORD
    );
    this.task = new MsSqlSourceTask();
    this.task.start(settings);
  }

  @Test
  public void foo() throws SQLException {
    this.task.query("dbo", "users");
  }

}
