package io.confluent.kafka.connect.cdc.mssql;

import com.google.common.base.Joiner;
import com.palantir.docker.compose.DockerComposeRule;
import com.palantir.docker.compose.connection.Container;
import com.palantir.docker.compose.connection.DockerPort;
import io.confluent.kafka.connect.cdc.JdbcUtils;
import io.confluent.kafka.connect.cdc.docker.DockerUtils;
import io.confluent.kafka.connect.cdc.docker.JdbcHealthCheck;
import org.flywaydb.core.Flyway;
import org.junit.ClassRule;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class DockerTest {
  public static final String USERNAME = "sa";
  public static final String PASSWORD = "1wRr8jZIxYlBVO";
  public static final int PORT = 1433;
  public static final String JDBCURL_FORMAT = "jdbc:sqlserver://$HOST:$EXTERNAL_PORT;databaseName=%s";
  public static final String CONTAINER_NAME = "mssql";
  public static final String DATABASE_NAME = "cdc_testing";
  @ClassRule
  public final static DockerComposeRule docker = DockerUtils.loadRule(
      "src/test/resources/docker-compose.yml", "target/mssql",
      CONTAINER_NAME, new JdbcHealthCheck(USERNAME, PASSWORD, PORT, String.format(JDBCURL_FORMAT, "master"))
  );

//  "jdbc:postgresql://$HOST:$EXTERNAL_PORT/CDC_TESTING"
  private static final Logger log = LoggerFactory.getLogger(DockerTest.class);

  public static Container mssqlContainer() {
    return docker.containers().container(CONTAINER_NAME);
  }

  public static String jdbcUrl(String databaseName) {
    String jdbcFormat = String.format(JDBCURL_FORMAT, databaseName);
    Container mssqlContainer = mssqlContainer();
    DockerPort port = mssqlContainer.port(PORT);
    return port.inFormat(jdbcFormat);
  }


  @BeforeAll
  public static void dockerComposeUp() throws IOException, InterruptedException, SQLException {
    docker.before();

    createDatabase();
    flywayMigrate();
  }

  static void createDatabase() throws SQLException {
    final String JDBCURL = jdbcUrl("master");
    try (Connection connection = JdbcUtils.openConnection(JDBCURL, USERNAME, PASSWORD)) {
      try (Statement statement = connection.createStatement()) {
        statement.execute("CREATE DATABASE cdc_testing");
      }
    }
  }

  static void flywayMigrate() throws SQLException {
    Flyway flyway = new Flyway();
    flyway.setDataSource(jdbcUrl("cdc_testing"), USERNAME, PASSWORD);
    flyway.setSchemas("dbo");
    if (log.isInfoEnabled()) {
      log.info("flyway locations. {}", Joiner.on(", ").join(flyway.getLocations()));
    }
    flyway.migrate();
  }

  @AfterAll
  public static void dockerCleanup() throws IOException, InterruptedException {
    docker.after();
  }


}
