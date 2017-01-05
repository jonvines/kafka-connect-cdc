package io.confluent.kafka.connect.cdc;

import com.google.common.collect.Maps;
import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.ConnectionPoolDataSource;
import javax.sql.PooledConnection;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

public class JdbcUtils {
  static final Map<ConnectionKey, ConnectionPoolDataSource> connections = Maps.newConcurrentMap();
  private static final Logger log = LoggerFactory.getLogger(JdbcUtils.class);


  private JdbcUtils() {
  }


  public static PooledConnection openPooledConnection(PooledCDCSourceConnectorConfig config, ChangeKey changeKey) {
    try {
      ConnectionKey connectionKey = ConnectionKey.of(
          config.serverName,
          config.serverPort,
          config.jdbcUsername,
          null == changeKey ? config.initialDatabase : changeKey.databaseName
      );
      if (log.isTraceEnabled()) {
        log.trace("{}: Getting ConnectionPoolDataSource for {}", changeKey, connectionKey);
      }

      ConnectionPoolDataSource dataSource = connections.computeIfAbsent(connectionKey, connectionKey1 -> {
        try {
          ConnectionPoolDataSourceFactory factory = config.connectionPoolDataSourceFactory();
          return factory.connectionPool(connectionKey);
        } catch (SQLException ex) {
          throw new DataException(
              String.format("Exception while creating factory for %s", changeKey),
              ex
          );
        }
      });

      return dataSource.getPooledConnection();
    } catch (SQLException ex) {
      throw new DataException("Exception thrown while connecting to database.", ex);
    }
  }

  public static void closeConnection(PooledConnection connection) throws SQLException {
    if (null != connection) {
      if (log.isTraceEnabled()) {
        log.trace("Returning connection to pool.");
      }
      connection.close();
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
