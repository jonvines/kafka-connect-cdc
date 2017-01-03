package io.confluent.kafka.connect.cdc;

import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class JdbcUtils {
  private static final Logger log = LoggerFactory.getLogger(JdbcUtils.class);

  private JdbcUtils() {
  }

  public static Connection openConnection(JdbcCDCSourceConnectorConfig config) {
    return openConnection(config.jdbcUrl, config.jdbcUsername, config.jdbcPassword);
  }

  public static Connection openConnection(String jdbcUrl, String jdbcUsername, String jdbcPassword) {
    try {
      if (log.isInfoEnabled()) {
        log.info("Connecting to {} as {}.", jdbcUrl, jdbcUsername);
      }
      return DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword);
    } catch (SQLException ex) {
      throw new DataException("Exception thrown while connecting to database.", ex);
    }
  }

  public static void closeConnection(Connection connection) {
    try {
      if (log.isInfoEnabled()) {
        log.info("closing...");
      }
      connection.close();
    } catch (Exception ex) {
      log.error("Exception thrown while closing connection", ex);
    }
  }
}
