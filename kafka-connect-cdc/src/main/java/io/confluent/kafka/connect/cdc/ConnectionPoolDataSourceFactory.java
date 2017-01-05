package io.confluent.kafka.connect.cdc;

import javax.sql.ConnectionPoolDataSource;
import java.sql.SQLException;

public interface ConnectionPoolDataSourceFactory {
  ConnectionPoolDataSource connectionPool(ConnectionKey connectionKey) throws SQLException;

  ConnectionKey connectionKey(ChangeKey changeKey);
}
