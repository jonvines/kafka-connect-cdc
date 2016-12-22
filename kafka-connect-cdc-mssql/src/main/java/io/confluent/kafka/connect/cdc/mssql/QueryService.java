package io.confluent.kafka.connect.cdc.mssql;

import com.google.common.util.concurrent.AbstractExecutionThreadService;
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

  @Override
  protected void run() throws Exception {
    while (isRunning()) {
      try {

      } catch (Exception ex) {

      }
    }
  }

  void queryTable(ChangeWriter changeWriter, String databaseName, String schemaName, String tableName) throws SQLException {
    try (Connection connection = JdbcUtils.openConnection(this.config)) {
      if (log.isDebugEnabled()) {
        log.debug("Setting transaction level to 4096 (READ_COMMITTED_SNAPSHOT)");
      }
      connection.setTransactionIsolation(4096);
      connection.setAutoCommit(false);

      TableMetadataProvider.TableMetadata tableMetadata = this.tableMetadataProvider.tableMetadata(databaseName, schemaName, tableName);
      MsSqlQueryBuilder queryBuilder = new MsSqlQueryBuilder(connection);

      try (PreparedStatement statement = queryBuilder.changeTrackingStatement(tableMetadata)) {
        statement.setLong(1, 0);

        long count = 0;

        try (ResultSet resultSet = statement.executeQuery()) {
          MsSqlChange change = null;

          while (resultSet.next()) {
            if (null != change) {
              changeWriter.addChange(change);
            }

            MsSqlChange.Builder builder = MsSqlChange.builder();
            change = builder.build(tableMetadata, resultSet, this.time);
            count++;
          }

          if (null != change) {
            changeWriter.addChange(change);
          }
        }

        if (log.isInfoEnabled()) {
          log.info("Processed {} record(s) for [{}].[{}].", count, schemaName, tableName);
        }

      } finally {
        connection.rollback();
      }
    }
  }
}
