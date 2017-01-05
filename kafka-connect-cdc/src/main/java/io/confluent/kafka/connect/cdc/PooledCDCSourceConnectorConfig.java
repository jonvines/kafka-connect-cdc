package io.confluent.kafka.connect.cdc;

import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public abstract class PooledCDCSourceConnectorConfig<T extends ConnectionPoolDataSourceFactory> extends CDCSourceConnectorConfig {
  public static final String JDBC_USERNAME_CONF = "username";
  public static final String JDBC_PASSWORD_CONF = "password";
  public static final String INITIAL_DATABASE_CONF = "initial.database";
  public static final String SERVER_NAME_CONF = "server.name";
  public static final String SERVER_PORT_CONF = "server.port";

  static final String INITIAL_DATABASE_DOC = "The initial database to connect to.";
  static final String SERVER_NAME_DOC = "The server to connect to.";
  static final String SERVER_PORT_DOC = "The port on the server to connect to.";

  static final String JDBC_USERNAME_DOC = "JDBC Username to connect to the database with.";
  static final String JDBC_PASSWORD_DOC = "JDBC Password to connect to the database with.";
  static final String JDBC_POOL_MAX_TOTAL_CONF = "jdbc.pool.max.total";
  static final String JDBC_POOL_MAX_TOTAL_DOC = "The maximum number of connections for the connection pool to open. If a number " +
      "greater than this value is requested, the caller will block waiting for a connection to be returned.";
  static final String JDBC_POOL_MAX_IDLE_CONF = "jdbc.pool.max.idle";
  static final String JDBC_POOL_MAX_IDLE_DOC = "The maximum number of idle connections in the connection pool.";
  static final String JDBC_POOL_MIN_IDLE_CONF = "jdbc.pool.min.idle";
  static final String JDBC_POOL_MIN_IDLE_DOC = "The minimum number of idle connections in the connection pool.";

  public final String serverName;
  public final int serverPort;
  public final String initialDatabase;

  public final String jdbcUsername;
  public final String jdbcPassword;
  public final int jdbcPoolMaxTotal;
  public final int jdbcPoolMaxIdle;
  public final int jdbcPoolMinIdle;


  public PooledCDCSourceConnectorConfig(ConfigDef definition, Map<?, ?> originals) {
    super(definition, originals);
    this.jdbcUsername = this.getString(JDBC_USERNAME_CONF);
    this.jdbcPassword = this.getPassword(JDBC_PASSWORD_CONF).value();
    this.jdbcPoolMaxTotal = this.getInt(JDBC_POOL_MAX_TOTAL_CONF);
    this.jdbcPoolMaxIdle = this.getInt(JDBC_POOL_MAX_IDLE_CONF);
    this.jdbcPoolMinIdle = this.getInt(JDBC_POOL_MIN_IDLE_CONF);
    this.serverName = this.getString(SERVER_NAME_CONF);
    this.serverPort = this.getInt(SERVER_PORT_CONF);
    this.initialDatabase = this.getString(INITIAL_DATABASE_CONF);
  }

  public static ConfigDef config() {
    return CDCSourceConnectorConfig.config()
        .define(SERVER_NAME_CONF, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, SERVER_NAME_DOC)
        .define(SERVER_PORT_CONF, ConfigDef.Type.INT, ConfigDef.Importance.HIGH, SERVER_PORT_DOC)
        .define(INITIAL_DATABASE_CONF, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, INITIAL_DATABASE_DOC)
        .define(JDBC_USERNAME_CONF, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, JDBC_USERNAME_DOC)
        .define(JDBC_PASSWORD_CONF, ConfigDef.Type.PASSWORD, ConfigDef.Importance.HIGH, JDBC_PASSWORD_DOC)
        .define(JDBC_POOL_MAX_TOTAL_CONF, ConfigDef.Type.INT, 30, ConfigDef.Importance.MEDIUM, JDBC_POOL_MAX_TOTAL_DOC)
        .define(JDBC_POOL_MAX_IDLE_CONF, ConfigDef.Type.INT, 10, ConfigDef.Importance.MEDIUM, JDBC_POOL_MAX_IDLE_DOC)
        .define(JDBC_POOL_MIN_IDLE_CONF, ConfigDef.Type.INT, 3, ConfigDef.Importance.MEDIUM, JDBC_POOL_MIN_IDLE_DOC);
  }

  public abstract T connectionPoolDataSourceFactory();

}
