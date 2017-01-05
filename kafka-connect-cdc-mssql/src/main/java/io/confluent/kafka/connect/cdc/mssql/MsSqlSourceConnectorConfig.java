package io.confluent.kafka.connect.cdc.mssql;

import com.google.common.collect.ImmutableList;
import io.confluent.kafka.connect.cdc.PooledCDCSourceConnectorConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.List;
import java.util.Map;

public class MsSqlSourceConnectorConfig extends PooledCDCSourceConnectorConfig<MsSqlConnectionPoolDataSourceFactory> {
  public static final String CHANGE_TRACKING_TABLES_CONFIG = "change.tracking.tables";
  static final String CHANGE_TRACKING_TABLES_DOC = "The tables in the source database to monitor for changes. " +
      "If no tables are specified the `[sys].[change_tracking_tables]` view is queried for all of the available tables " +
      "with change tracking enabled.";
  static final List<String> CHANGE_TRACKING_TABLES_DEFAULT = ImmutableList.of();

  public final List<String> changeTrackingTables;

  private final MsSqlConnectionPoolDataSourceFactory connectionPoolDataSourceFactory;

  public MsSqlSourceConnectorConfig(Map<String, String> parsedConfig) {
    super(config(), parsedConfig);
    this.changeTrackingTables = this.getList(CHANGE_TRACKING_TABLES_CONFIG);
    this.connectionPoolDataSourceFactory = new MsSqlConnectionPoolDataSourceFactory(this);
  }

  public static ConfigDef config() {
    return PooledCDCSourceConnectorConfig.config()
        .define(CHANGE_TRACKING_TABLES_CONFIG, ConfigDef.Type.LIST, CHANGE_TRACKING_TABLES_DEFAULT, ConfigDef.Importance.MEDIUM, CHANGE_TRACKING_TABLES_DOC)
        ;
  }

  @Override
  public MsSqlConnectionPoolDataSourceFactory connectionPoolDataSourceFactory() {
    return this.connectionPoolDataSourceFactory;
  }
}