package io.confluent.kafka.connect.cdc.mssql.docker;

import io.confluent.kafka.connect.cdc.docker.JdbcClusterHealthCheck;
import io.confluent.kafka.connect.cdc.mssql.MsSqlTestConstants;

public class MsSqlClusterHealthCheck extends JdbcClusterHealthCheck {
  public MsSqlClusterHealthCheck() {
    super(MsSqlTestConstants.CONTAINER_NAME,
        MsSqlTestConstants.PORT,
        MsSqlTestConstants.JDBCURL_FORMAT_MASTER,
        MsSqlTestConstants.USERNAME,
        MsSqlTestConstants.PASSWORD);
  }
}
