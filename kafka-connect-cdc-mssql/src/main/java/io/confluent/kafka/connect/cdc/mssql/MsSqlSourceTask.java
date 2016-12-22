package io.confluent.kafka.connect.cdc.mssql;

import com.google.common.util.concurrent.ServiceManager;
import io.confluent.kafka.connect.cdc.CDCSourceTask;
import io.confluent.kafka.connect.cdc.TableMetadataProvider;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class MsSqlSourceTask extends CDCSourceTask<MsSqlSourceConnectorConfig> {
  private static final Logger log = LoggerFactory.getLogger(MsSqlSourceTask.class);
  Time time = new SystemTime();
  ServiceManager serviceManager;
  private TableMetadataProvider tableMetadataProvider;

  @Override
  protected MsSqlSourceConnectorConfig getConfig(Map<String, String> map) {
    return new MsSqlSourceConnectorConfig(map);
  }

  @Override
  public void start(Map<String, String> map) {
    super.start(map);
    this.tableMetadataProvider = new MsSqlTableMetadataProvider(this.config, context.offsetStorageReader());
    QueryService queryService = new QueryService(this.time, this.tableMetadataProvider, this.config, this);
    this.serviceManager = new ServiceManager(Arrays.asList(queryService));
    this.serviceManager.startAsync();
  }

  @Override
  public void stop() {
    if (log.isInfoEnabled()) {
      log.info("Queries in flight can take a while to finish. No more queries will be issued.");
    }
    this.serviceManager.stopAsync();

    try {
      this.serviceManager.awaitStopped(5, TimeUnit.MINUTES);
    } catch (TimeoutException e) {
      if (log.isErrorEnabled()) {
        log.error("Timeout exceeded waiting for the service to stop.");
      }
    }
  }
}
