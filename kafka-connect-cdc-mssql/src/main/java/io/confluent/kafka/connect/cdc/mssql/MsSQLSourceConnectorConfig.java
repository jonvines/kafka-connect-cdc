package io.confluent.kafka.connect.cdc.mssql;

import io.confluent.kafka.connect.cdc.JDBCCDCSourceConnectorConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class MsSQLSourceConnectorConfig extends JDBCCDCSourceConnectorConfig {
  public MsSQLSourceConnectorConfig(Map<String, String> parsedConfig) {
    super(config(), parsedConfig);
  }

  public static ConfigDef config() {
    return JDBCCDCSourceConnectorConfig.config();
  }
}