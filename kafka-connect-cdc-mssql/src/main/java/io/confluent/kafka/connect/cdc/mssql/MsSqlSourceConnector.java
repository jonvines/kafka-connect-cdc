package io.confluent.kafka.connect.cdc.mssql;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import io.confluent.kafka.connect.cdc.CDCSourceConnector;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class MsSqlSourceConnector extends CDCSourceConnector {

  Map<String, String> settings;
  MsSqlSourceConnectorConfig config;

  @Override
  public void start(Map<String, String> settings) {
    this.settings = settings;
    this.config = new MsSqlSourceConnectorConfig(settings);
  }

  @Override
  public Class<? extends Task> taskClass() {
    return MsSqlSourceTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int taskCount) {
    Preconditions.checkState(taskCount > 0, "At least one task is required");

    List<Map<String, String>> taskConfigs = new ArrayList<>(taskCount);
    for (Iterable<String> tables : Iterables.partition(this.config.changeTrackingTables, taskCount)) {
      Map<String, String> taskSettings = new LinkedHashMap<>();
      taskSettings.putAll(this.settings);
      taskSettings.put(MsSqlSourceConnectorConfig.CHANGE_TRACKING_TABLES_CONFIG, Joiner.on(',').join(tables));
      taskConfigs.add(taskSettings);
    }

    return taskConfigs;
  }

  @Override
  public void stop() {

  }

  @Override
  public ConfigDef config() {
    return MsSqlSourceConnectorConfig.config();
  }
}