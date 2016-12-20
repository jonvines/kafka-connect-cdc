package io.confluent.kafka.connect.cdc;

import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class JdbcCDCSourceConnectorConfig extends CDCSourceConnectorConfig {
  public static final String JDBC_USERNAME_CONF = "jdbc.username";
  public static final String JDBC_PASSWORD_CONF = "jdbc.password";
  public static final String JDBC_URL_CONF = "jdbc.url";
  static final String JDBC_USERNAME_DOC = "JDBC Username to connect to the database with.";
  static final String JDBC_PASSWORD_DOC = "JDBC Password to connect to the database with.";
  static final String JDBC_URL_DOC = "JDBC Url to connect to oracle with. You should never inline your username and password.";

  public final String jdbcUrl;
  public final String jdbcUsername;
  public final String jdbcPassword;

  public JdbcCDCSourceConnectorConfig(ConfigDef definition, Map<?, ?> originals) {
    super(definition, originals);
    this.jdbcUrl = this.getString(JDBC_URL_CONF);
    this.jdbcUsername = this.getString(JDBC_USERNAME_CONF);
    this.jdbcPassword = this.getPassword(JDBC_PASSWORD_CONF).value();
  }

  public static ConfigDef config() {
    return CDCSourceConnectorConfig.config()
        .define(JDBC_URL_CONF, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, JDBC_URL_DOC)
        .define(JDBC_USERNAME_CONF, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, JDBC_USERNAME_DOC)
        .define(JDBC_PASSWORD_CONF, ConfigDef.Type.PASSWORD, ConfigDef.Importance.HIGH, JDBC_PASSWORD_DOC);
  }
}
