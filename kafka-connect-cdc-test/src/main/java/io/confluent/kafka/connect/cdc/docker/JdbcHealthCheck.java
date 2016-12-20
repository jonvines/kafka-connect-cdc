package io.confluent.kafka.connect.cdc.docker;

import com.palantir.docker.compose.connection.Container;
import com.palantir.docker.compose.connection.DockerPort;
import com.palantir.docker.compose.connection.waiting.Attempt;
import com.palantir.docker.compose.connection.waiting.HealthCheck;
import com.palantir.docker.compose.connection.waiting.SuccessOrFailure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;

public class JdbcHealthCheck implements HealthCheck<Container> {
  private static final Logger log = LoggerFactory.getLogger(JdbcHealthCheck.class);
  final String username;
  final String password;
  final String jdbcUrlFormat;
  final int internalPort;

  public JdbcHealthCheck(String username, String password, int internalPort, String jdbcUrlFormat) {
    this.username = username;
    this.password = password;
    this.jdbcUrlFormat = jdbcUrlFormat;
    this.internalPort = internalPort;
  }

  @Override
  public SuccessOrFailure isHealthy(Container container) {
    final DockerPort port = container.port(this.internalPort);
    final String jdbcUrl = port.inFormat(this.jdbcUrlFormat);

    if (log.isInfoEnabled()) {
      log.info("Attempting to authenticate to {} with user {}.", jdbcUrl, this.username);
    }

    return SuccessOrFailure.onResultOf(
        new Attempt() {
          @Override
          public boolean attempt() throws Exception {
            try (Connection connection = DriverManager.getConnection(
                jdbcUrl,
                username,
                password
            )) {
              return true;
            } catch (Exception ex) {
              if (log.isDebugEnabled()) {
                log.debug("Exception thrown", ex);
              }

              Thread.sleep(2000);
              return false;
            }

          }
        }
    );
  }
}
