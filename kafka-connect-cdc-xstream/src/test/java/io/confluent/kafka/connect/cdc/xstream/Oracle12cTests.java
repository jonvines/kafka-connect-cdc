package io.confluent.kafka.connect.cdc.xstream;

import com.palantir.docker.compose.connection.waiting.ClusterHealthCheck;
import io.confluent.kafka.connect.cdc.docker.DockerFormatString;
import io.confluent.kafka.connect.cdc.xstream.docker.Oracle12cClusterHealthCheck;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;

public class Oracle12cTests {
  public static final String DOCKER_COMPOSE_FILE = "src/test/resources/docker-compose-12c.yml";
  public static final Class<? extends ClusterHealthCheck> CLUSTER_HEALTH_CHECK_CLASS = Oracle12cClusterHealthCheck.class;
  private static final Logger log = LoggerFactory.getLogger(Oracle12cTests.class);

  @BeforeAll
  public static void beforeClass(
      @DockerFormatString(container = XStreamConstants.ORACLE_CONTAINER, port = XStreamConstants.ORACLE_PORT, format = XStreamConstants.JDBC_URL_FORMAT_12C_PDB) String jdbcUrl
  ) throws SQLException, InterruptedException, IOException {
    flywayMigrate(jdbcUrl, "db/migration/common", "db/migration/oracle12c");
  }

  static void flywayMigrate(String jdbcUrl, String... locations) throws SQLException {
    Flyway flyway = new Flyway();
    flyway.setDataSource(jdbcUrl, XStreamConstants.USERNAME, XStreamConstants.PASSWORD);
    flyway.setSchemas("DATATYPE_TESTING");
    flyway.setLocations(locations);
    flyway.migrate();
  }

}