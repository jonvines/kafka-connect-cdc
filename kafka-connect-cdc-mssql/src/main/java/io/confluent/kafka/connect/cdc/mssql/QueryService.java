package io.confluent.kafka.connect.cdc.mssql;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.RateLimiter;
import io.confluent.kafka.connect.cdc.Change;
import io.confluent.kafka.connect.cdc.ChangeKey;
import io.confluent.kafka.connect.cdc.ChangeWriter;
import io.confluent.kafka.connect.cdc.JdbcUtils;
import io.confluent.kafka.connect.cdc.TableMetadataProvider;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class QueryService extends AbstractExecutionThreadService {
  private static final Logger log = LoggerFactory.getLogger(QueryService.class);

  final Time time;
  final TableMetadataProvider tableMetadataProvider;
  final MsSqlSourceConnectorConfig config;
  final ChangeWriter changeWriter;

  QueryService(Time time, TableMetadataProvider tableMetadataProvider, MsSqlSourceConnectorConfig config, ChangeWriter changeWriter) {
    this.time = time;
    this.tableMetadataProvider = tableMetadataProvider;
    this.config = config;
    this.changeWriter = changeWriter;
  }

  static final Pattern CHANGE_TRACKING_TABLE_PATTERN = Pattern.compile("^([^\\.]+)\\.([^\\.]+)\\.([^\\.]+)$");

  ChangeKey changeKey(String changeTrackingTable) {
    Matcher matcher = CHANGE_TRACKING_TABLE_PATTERN.matcher(changeTrackingTable);
    Preconditions.checkState(matcher.matches(), "'%s' is not formatted in properly. Use 'schemaName.databaseName.tableName'.");
    String databaseName = matcher.group(1);
    String schemaName = matcher.group(2);
    String tableName = matcher.group(3);
    return new ChangeKey(databaseName, schemaName, tableName);
  }

  @Override
  protected void run() throws Exception {
    while (isRunning()) {
      try {
        processTables();
      } catch (Exception ex) {
        if (log.isErrorEnabled()) {
          log.error("Exception thrown", ex);
        }
      }
    }
  }

  RateLimiter rateLimiter = RateLimiter.create(1);

  void processTables() throws SQLException {
    for (String changeTrackingTable : Iterables.cycle(this.config.changeTrackingTables)) {
      if (!isRunning()) {
        break;
      }

      rateLimiter.acquire();

      ChangeKey changeKey;

      try {
        changeKey = changeKey(changeTrackingTable);
      } catch (Exception ex) {
        if (log.isErrorEnabled()) {
          log.error("Exception thrown while parsing table name '{}'", changeTrackingTable, ex);
        }
        continue;
      }

      try {
        queryTable(this.changeWriter, changeKey);
      } catch (Exception ex) {
        if (log.isErrorEnabled()) {
          log.error("Exception thrown while querying for {}", changeKey, ex);
        }
      }
    }
  }

  void queryTable(ChangeWriter changeWriter, ChangeKey changeKey) throws SQLException {
    try (Connection connection = JdbcUtils.openConnection(this.config)) {
      if (log.isDebugEnabled()) {
        log.debug("Setting transaction level to 4096 (READ_COMMITTED_SNAPSHOT)");
      }
      connection.setTransactionIsolation(4096);
      connection.setAutoCommit(false);

      TableMetadataProvider.TableMetadata tableMetadata = this.tableMetadataProvider.tableMetadata(changeKey);
      MsSqlQueryBuilder queryBuilder = new MsSqlQueryBuilder(connection);

      Map<String, Object> sourcePartition = Change.sourcePartition(changeKey);
      Map<String, Object> startOffset = this.tableMetadataProvider.startOffset(changeKey);
      long offset = MsSqlChange.offset(startOffset);

      if (log.isDebugEnabled()) {
        log.debug("Starting at offset {} for ", offset, changeKey);
      }

      try (PreparedStatement statement = queryBuilder.changeTrackingStatement(tableMetadata)) {
        statement.setLong(1, offset);

        long count = 0;

        try (ResultSet resultSet = statement.executeQuery()) {

          long changeVersion = 0;
          while (resultSet.next()) {
            changeVersion = resultSet.getLong("__metadata_sys_change_version");

            if(log.isDebugEnabled()) {
              log.debug("__metadata_sys_change_version = {} for {}", changeVersion, changeKey);
            }

            MsSqlChange.Builder builder = MsSqlChange.builder();
            MsSqlChange change = builder.build(tableMetadata, resultSet, this.time);
            change.sourcePartition = sourcePartition;
            change.sourceOffset = MsSqlChange.offset(changeVersion);
            changeWriter.addChange(change);
            this.tableMetadataProvider.cacheOffset(changeKey, change.sourceOffset);
            count++;
          }
        }

        if (log.isInfoEnabled()) {
          log.info("Processed {} record(s) for [{}].", count, changeKey);
        }

      } finally {
        if(log.isDebugEnabled()) {
          log.debug("calling connection.commit()");
        }
        connection.commit();
      }
    }
  }
}
