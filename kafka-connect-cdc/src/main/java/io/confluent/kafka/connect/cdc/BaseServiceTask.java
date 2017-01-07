package io.confluent.kafka.connect.cdc;

import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.ServiceManager;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public abstract class BaseServiceTask<Conf extends CDCSourceConnectorConfig> extends CDCSourceTask<Conf> {
  private static final Logger log = LoggerFactory.getLogger(BaseServiceTask.class);

  protected abstract Service service(ChangeWriter changeWriter, OffsetStorageReader offsetStorageReader);

  ServiceManager serviceManager;

  @Override
  public void start(Map<String, String> map) {
    super.start(map);
    Service service = service(this, this.context.offsetStorageReader());
    List<Service> services = Arrays.asList(service);
    this.serviceManager = new ServiceManager(services);

    if (log.isInfoEnabled()) {
      log.info("Starting Services");
    }
    this.serviceManager.startAsync();

    try {
      this.serviceManager.awaitHealthy(60, TimeUnit.SECONDS);
    } catch (TimeoutException e) {
      throw new ConnectException("Timeout while starting service.", e);
    }
  }

  @Override
  public void stop() {
    if (log.isInfoEnabled()) {
      log.info("Stopping Services");
    }
    this.serviceManager.stopAsync();

    try {
      this.serviceManager.awaitStopped(60, TimeUnit.SECONDS);
    } catch (TimeoutException e) {
      throw new ConnectException("Timeout while stopping service.", e);
    }

  }
}
