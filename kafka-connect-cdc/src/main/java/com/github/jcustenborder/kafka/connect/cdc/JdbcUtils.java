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
  static final Map<ConnectionKey, ConnectionPoolDataSource> CONNECTIONS = Maps.newConcurrentMap();
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
      log.trace("openPooledConnection() - {}: Getting ConnectionPoolDataSource for {}", changeKey, connectionKey);

      ConnectionPoolDataSource dataSource = CONNECTIONS.computeIfAbsent(connectionKey, connectionKey1 -> {
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
      log.trace("closeConnection() - Returning connection to pool.");
      connection.close();
    }
  }


  public static void closeConnection(Connection connection) {
    try {
      log.info("closing...");
      connection.close();
    } catch (Exception ex) {
      log.error("Exception thrown while closing connection", ex);
    }
  }
}
