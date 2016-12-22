package io.confluent.kafka.connect.cdc.mssql;

import com.google.common.util.concurrent.ServiceManager;
import io.confluent.kafka.connect.cdc.CDCSourceTask;
import io.confluent.kafka.connect.cdc.ChangeWriter;
import io.confluent.kafka.connect.cdc.JdbcUtils;
import io.confluent.kafka.connect.cdc.TableMetadataProvider;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

public class MsSqlSourceTask extends CDCSourceTask<MsSqlSourceConnectorConfig> implements Runnable {
  private static final Logger log = LoggerFactory.getLogger(MsSqlSourceTask.class);
  Time time = new SystemTime();
  private TableMetadataProvider tableMetadataProvider;

  ServiceManager serviceManager;


  @Override
  protected MsSqlSourceConnectorConfig getConfig(Map<String, String> map) {
    return new MsSqlSourceConnectorConfig(map);
  }

  @Override
  public void start(Map<String, String> map) {
    super.start(map);



    this.tableMetadataProvider = new MsSqlTableMetadataProvider(this.config, context.offsetStorageReader());
  }

  @Override
  public void stop() {

  }

  @Override
  public void run() {
    while (true) {
      try {
//        queryTable("dbo", "users");
      } catch (Exception ex) {

      }
    }
  }

//  void queryTable(ChangeWriter changeWriter, String databaseName, String schemaName, String tableName) throws SQLException {
//    try (Connection connection = JdbcUtils.openConnection(this.config)) {
//      if (log.isDebugEnabled()) {
//        log.debug("Setting transaction level to 4096 (READ_COMMITTED_SNAPSHOT)");
//      }
//      connection.setTransactionIsolation(4096);
//      connection.setAutoCommit(false);
//
//      TableMetadataProvider.TableMetadata tableMetadata = this.tableMetadataProvider.tableMetadata(databaseName, schemaName, tableName);
//      MsSqlQueryBuilder queryBuilder = new MsSqlQueryBuilder(connection);
//
//      try (PreparedStatement statement = queryBuilder.changeTrackingStatement(tableMetadata)) {
//        statement.setLong(1, 0);
//
//        long count = 0;
//
//        try (ResultSet resultSet = statement.executeQuery()) {
//          MsSqlChange change = null;
//
//          while (resultSet.next()) {
//            if (null != change) {
//              changeWriter.addChange(change);
//            }
//
//            MsSqlChange.Builder builder = MsSqlChange.builder();
//            change = builder.build(tableMetadata, resultSet, this.time);
//            count++;
//          }
//
//          if (null != change) {
//            changeWriter.addChange(change);
//          }
//        }
//
//        if (log.isInfoEnabled()) {
//          log.info("Processed {} record(s) for [{}].[{}].", count, schemaName, tableName);
//        }
//
//      } finally {
//        connection.rollback();
//      }
//    }
//  }


}
