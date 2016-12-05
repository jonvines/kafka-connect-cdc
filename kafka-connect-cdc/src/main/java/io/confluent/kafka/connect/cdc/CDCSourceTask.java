package io.confluent.kafka.connect.cdc;

import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;

public abstract class CDCSourceTask<Conf extends CDCSourceConnectorConfig> extends SourceTask {
  private static final Logger log = LoggerFactory.getLogger(CDCSourceTask.class);
  private ConcurrentLinkedDeque<Change> changes;
  protected Conf config;
  protected Time time = new SystemTime();

  SchemaGenerater schemaGenerater;

  protected abstract Conf getConfig(Map<String, String> map);

  void addChange(Change change) {
    if (log.isDebugEnabled()) {
      log.debug("Adding change {}", change);
    }

    this.changes.add(change);
  }

  @Override
  public void start(Map<String, String> map) {
    this.config = getConfig(map);
    this.changes = new ConcurrentLinkedDeque<>();
    this.schemaGenerater = new SchemaGenerater(this.config);
  }

  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    Change change;
    List<SourceRecord> records = new ArrayList<>(this.config.batchSize);

    while ((change = this.changes.poll()) != null) {
      if (records.size() >= this.config.batchSize) {
        if (log.isDebugEnabled()) {
          log.debug("Exceeded batch size of {}, returning.", records.size());
        }

        SchemaPair schemaPair = this.schemaGenerater.generateSchemas(change);
        StructPair structPair = new StructPair(schemaPair);


        break;
      }
    }

    if (records.isEmpty()) {
      if (log.isDebugEnabled()) {
        log.debug("No records returned sleeping for {} ms", this.config.backoffTimeMs);
      }
      time.sleep(this.config.backoffTimeMs);
    }

    return records;
  }
}
