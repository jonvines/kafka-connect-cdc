package io.confluent.kafka.connect.cdc.mssql;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.connect.cdc.Change;
import io.confluent.kafka.connect.cdc.ChangeKey;
import io.confluent.kafka.connect.cdc.ChangeWriter;
import io.confluent.kafka.connect.cdc.JdbcUtils;
import io.confluent.kafka.connect.cdc.JsonChangeList;
import org.apache.kafka.common.utils.Time;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static io.confluent.kafka.connect.cdc.ChangeAssertions.assertChange;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class MsSqlSourceTaskTest extends DockerTest {
  private static final Logger log = LoggerFactory.getLogger(MsSqlSourceTaskTest.class);
  MsSqlSourceTask task;
  MsSqlSourceConnectorConfig config;

  @BeforeEach
  public void before() {
    Map<String, String> settings = ImmutableMap.of(
        MsSqlSourceConnectorConfig.JDBC_URL_CONF, jdbcUrl(DATABASE_NAME),
        MsSqlSourceConnectorConfig.JDBC_USERNAME_CONF, USERNAME,
        MsSqlSourceConnectorConfig.JDBC_PASSWORD_CONF, PASSWORD
    );
    this.config = new MsSqlSourceConnectorConfig(settings);
    this.task = new MsSqlSourceTask();
    this.task.start(settings);
  }

  @TestFactory
  public Stream<DynamicTest> queryTable() throws SQLException {
    List<ChangeKey> changeCaptureTables = new ArrayList<>();
    try (Connection connection = JdbcUtils.openConnection(this.config)) {
      MsSqlQueryBuilder queryBuilder = new MsSqlQueryBuilder(connection);
      try (PreparedStatement statement = queryBuilder.listChangeTrackingTablesStatement()) {
        try (ResultSet resultSet = statement.executeQuery()) {
          while (resultSet.next()) {
            String databaseName = resultSet.getString("databaseName");
            String schemaName = resultSet.getString("schemaName");
            String tableName = resultSet.getString("tableName");
            ChangeKey changeKey = new ChangeKey(databaseName, schemaName, tableName);
            changeCaptureTables.add(changeKey);
            if (log.isDebugEnabled()) {
              log.debug("Found Change Tracking Enabled Table {}", changeKey);
            }
          }
        }
      }
    }

    return changeCaptureTables.stream().map(data -> dynamicTest(data.tableName, () -> queryTable(data)));
  }

  private void queryTable(ChangeKey input) throws SQLException, IOException {
    final List<Change> actualChanges = new ArrayList<>(1000);
    ChangeWriter changeWriter = mock(ChangeWriter.class);

    doAnswer(invocationOnMock -> {
      Change change = invocationOnMock.getArgumentAt(0, Change.class);
      actualChanges.add(change);
      return null;
    }).when(changeWriter).addChange(any());

    JsonChangeList expectedChanges;
    String resourceName = String.format("query/table/%s/%s.%s.json", input.databaseName, input.schemaName, input.tableName);
    long timestamp = 0L;
    try (InputStream stream = this.getClass().getResourceAsStream(resourceName)) {
      Preconditions.checkNotNull(stream, "Could not find resource %s.", resourceName);
      if (log.isInfoEnabled()) {
        log.info("Loading expected changes from {}", resourceName);
      }
      expectedChanges = JsonChangeList.read(stream);
      for (Change change : expectedChanges) {
        timestamp = change.timestamp();
        break;
      }
    }


    this.task.time = mock(Time.class);
    when(this.task.time.milliseconds()).thenReturn(timestamp);
    this.task.queryTable(changeWriter, input.databaseName, input.schemaName, input.tableName);

    assertFalse(actualChanges.isEmpty(), "Changes should have been returned.");
    assertEquals(expectedChanges.size(), actualChanges.size(), "The number of changes returned is not the expect count.");
    for (int i = 0; i < expectedChanges.size(); i++) {
      Change expectedChange = expectedChanges.get(i);
      Change actualChange = actualChanges.get(i);
      assertChange(expectedChange, actualChange);
    }

  }

}
