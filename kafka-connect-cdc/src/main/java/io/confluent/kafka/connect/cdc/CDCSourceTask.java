package io.confluent.kafka.connect.cdc;

import com.google.common.base.Preconditions;
import io.confluent.kafka.connect.utils.data.SourceRecordConcurrentLinkedDeque;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public abstract class CDCSourceTask<Conf extends CDCSourceConnectorConfig> extends SourceTask implements ChangeWriter {
  private static final Logger log = LoggerFactory.getLogger(CDCSourceTask.class);
  protected Conf config;
  protected Time time = new SystemTime();
  SchemaGenerator schemaGenerator;
  private SourceRecordConcurrentLinkedDeque changes;

  protected abstract Conf getConfig(Map<String, String> map);


  void setStructField(Struct struct, String field, Object value) {
    try {
      struct.put(field, value);
    } catch (DataException ex) {
      throw new DataException(
          String.format("Exception thrown while setting the value for field '%s'. data=%s",
              field,
              null == value ? "NULL" : value.getClass()
          ),
          ex
      );
    }
  }

  SourceRecord createRecord(SchemaPair schemaPair, Change change) {
    Preconditions.checkNotNull(change.metadata(), "change.metadata() cannot return null.");
    StructPair structPair = new StructPair(schemaPair);
    for (int i = 0; i < schemaPair.getKey().fields.size(); i++) {
      String fieldName = schemaPair.getKey().fields.get(i);
      Change.ColumnValue columnValue = change.keyColumns().get(i);
      setStructField(structPair.getKey(), fieldName, columnValue.value());
    }

    for (int i = 0; i < schemaPair.getValue().fields.size(); i++) {
      String fieldName = schemaPair.getValue().fields.get(i);
      Change.ColumnValue columnValue = change.valueColumns().get(i);
      setStructField(structPair.getValue(), fieldName, columnValue.value());
    }

    Map<String, String> metadata = new LinkedHashMap<>(change.metadata().size() + 3);
    metadata.putAll(change.metadata());
    metadata.put("schemaName", change.schemaName());
    metadata.put("tableName", change.tableName());
    setStructField(structPair.getValue(), Constants.METADATA_FIELD, change.metadata());

    //TODO: Correct topic pattern

    SourceRecord sourceRecord = new SourceRecord(
        change.sourcePartition(),
        change.sourceOffset(),
        "topic",
        null,
        schemaPair.getKey().schema,
        structPair.getKey(),
        schemaPair.getValue().schema,
        structPair.getValue(),
        change.timestamp()
    );

    return sourceRecord;
  }

  @Override
  public void addChange(Change change) {
    if (log.isDebugEnabled()) {
      log.debug("Adding change {}", change);
    }
    if (Change.ChangeType.DELETE == change.changeType()) {
      if (log.isDebugEnabled()) {
        log.debug("Dropping delete. This will be potentially supported later.");
      }
      return;
    }
    SchemaPair schemaPair = this.schemaGenerator.get(change);
    SourceRecord record = createRecord(schemaPair, change);
    this.changes.add(record);
  }

  @Override
  public void start(Map<String, String> map) {
    this.config = getConfig(map);
    this.changes = new SourceRecordConcurrentLinkedDeque(this.config.batchSize, this.config.backoffTimeMs);
    this.schemaGenerator = new SchemaGenerator(this.config);
  }


  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    List<SourceRecord> records = new ArrayList<>(this.config.batchSize);

    if (this.changes.drain(records)) {
      return records;
    }

    return records;
  }

  @Override
  public String version() {
    return VersionUtil.getVersion();
  }
}
