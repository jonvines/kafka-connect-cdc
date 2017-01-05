package io.confluent.kafka.connect.cdc.mssql;

import com.google.common.base.Strings;
import com.microsoft.sqlserver.jdbc.SQLServerConnectionPoolDataSource;
import io.confluent.kafka.connect.cdc.ChangeKey;
import io.confluent.kafka.connect.cdc.ConnectionKey;
import io.confluent.kafka.connect.cdc.ConnectionPoolDataSourceFactory;

import javax.sql.ConnectionPoolDataSource;
import java.sql.SQLException;

class MsSqlConnectionPoolDataSourceFactory implements ConnectionPoolDataSourceFactory {
  private final MsSqlSourceConnectorConfig config;

  public MsSqlConnectionPoolDataSourceFactory(MsSqlSourceConnectorConfig config) {
    this.config = config;
  }

  @Override
  public ConnectionPoolDataSource connectionPool(ConnectionKey connectionKey) throws SQLException {
    SQLServerConnectionPoolDataSource dataSource = new SQLServerConnectionPoolDataSource();
    dataSource.setServerName(this.config.serverName);
    dataSource.setPortNumber(this.config.serverPort);
    dataSource.setUser(this.config.jdbcUsername);
    dataSource.setPassword(this.config.jdbcPassword);

    if (Strings.isNullOrEmpty(connectionKey.databaseName)) {
      dataSource.setDatabaseName(this.config.initialDatabase);
    } else {
      dataSource.setDatabaseName(connectionKey.databaseName);
    }

    return dataSource;
  }

  @Override
  public ConnectionKey connectionKey(ChangeKey changeKey) {
    return ConnectionKey.of(this.config.serverName, this.config.serverPort, this.config.jdbcUsername, changeKey.databaseName);
  }
}
