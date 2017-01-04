package io.confluent.kafka.connect.cdc.mssql;

import com.google.common.base.Joiner;
import io.confluent.kafka.connect.cdc.JdbcUtils;
import io.confluent.kafka.connect.cdc.docker.DockerFormatString;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class MsSqlTests {
  private static final Logger log = LoggerFactory.getLogger(MsSqlTests.class);

  @BeforeAll
  public static void beforeClass(
      @DockerFormatString(container = MsSqlTestConstants.CONTAINER_NAME, port = MsSqlTestConstants.PORT, format = MsSqlTestConstants.JDBCURL_FORMAT_MASTER) String masterJdbcUrl,
      @DockerFormatString(container = MsSqlTestConstants.CONTAINER_NAME, port = MsSqlTestConstants.PORT, format = MsSqlTestConstants.JDBCURL_FORMAT_CDC_TESTING) String cdcTestingJdbcUrl
  ) throws SQLException, InterruptedException, IOException {
    createDatabase(masterJdbcUrl);
    flywayMigrate(cdcTestingJdbcUrl);
  }

  static void createDatabase(final String jdbcUrl) throws SQLException {
    try (Connection connection = JdbcUtils.openConnection(jdbcUrl, MsSqlTestConstants.USERNAME, MsSqlTestConstants.PASSWORD)) {
      try (Statement statement = connection.createStatement()) {
        statement.execute("CREATE DATABASE cdc_testing");
      }
    }
  }

  static void flywayMigrate(final String jdbcUrl) throws SQLException {
    Flyway flyway = new Flyway();
    flyway.setDataSource(jdbcUrl, MsSqlTestConstants.USERNAME, MsSqlTestConstants.PASSWORD);
    flyway.setSchemas("dbo");
    if (log.isInfoEnabled()) {
      log.info("flyway locations. {}", Joiner.on(", ").join(flyway.getLocations()));
    }
    flyway.migrate();
  }
}
