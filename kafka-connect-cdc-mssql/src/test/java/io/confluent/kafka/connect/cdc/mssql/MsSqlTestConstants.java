package io.confluent.kafka.connect.cdc.mssql;

public class MsSqlTestConstants {
  public static final String USERNAME = "sa";
  public static final String PASSWORD = "1wRr8jZIxYlBVO";
  public static final int PORT = 1433;
  public static final String JDBCURL_FORMAT_MASTER = "jdbc:sqlserver://$HOST:$EXTERNAL_PORT;databaseName=master";
  public static final String JDBCURL_FORMAT_CDC_TESTING = "jdbc:sqlserver://$HOST:$EXTERNAL_PORT;databaseName=cdc_testing";
  public static final String CONTAINER_NAME = "mssql";
  public static final String DATABASE_NAME = "cdc_testing";
  public static final String DOCKER_COMPOSE_FILE = "src/test/resources/docker-compose.yml";

}
