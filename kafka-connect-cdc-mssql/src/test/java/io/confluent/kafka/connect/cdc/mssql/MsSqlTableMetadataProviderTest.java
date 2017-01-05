package io.confluent.kafka.connect.cdc.mssql;

import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.connect.cdc.ChangeKey;
import io.confluent.kafka.connect.cdc.Integration;
import io.confluent.kafka.connect.cdc.TableMetadataProvider;
import io.confluent.kafka.connect.cdc.TestDataUtils;
import io.confluent.kafka.connect.cdc.docker.DockerCompose;
import io.confluent.kafka.connect.cdc.docker.DockerFormatString;
import io.confluent.kafka.connect.cdc.mssql.docker.MsSqlClusterHealthCheck;
import io.confluent.kafka.connect.cdc.mssql.docker.MsSqlSettings;
import io.confluent.kafka.connect.cdc.mssql.docker.MsSqlSettingsExtension;
import io.confluent.kafka.connect.cdc.mssql.model.MsSqlTableMetadataProviderTestData;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static io.confluent.kafka.connect.cdc.ChangeAssertions.assertTableMetadata;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;
import static org.mockito.Mockito.mock;

@Category(Integration.class)
@DockerCompose(dockerComposePath = MsSqlTestConstants.DOCKER_COMPOSE_FILE, clusterHealthCheck = MsSqlClusterHealthCheck.class)
@ExtendWith(MsSqlSettingsExtension.class)
public class MsSqlTableMetadataProviderTest extends MsSqlTest {
  MsSqlSourceConnectorConfig config;
  TableMetadataProvider tableMetadataProvider;
  OffsetStorageReader offsetStorageReader;

  @BeforeEach
  public void before(@MsSqlSettings Map<String, String> settings) {
    this.config = new MsSqlSourceConnectorConfig(settings);
    this.offsetStorageReader = mock(OffsetStorageReader.class);
    this.tableMetadataProvider = new MsSqlTableMetadataProvider(this.config, this.offsetStorageReader);
  }

  @TestFactory
  public Stream<DynamicTest> tableMetadata() throws IOException {
    String packageName = this.getClass().getPackage().getName() + ".metadata.table";
    List<MsSqlTableMetadataProviderTestData> testData = TestDataUtils.loadJsonResourceFiles(packageName, MsSqlTableMetadataProviderTestData.class);
    return testData.stream().map(data -> dynamicTest(data.name(), () -> tableMetadata(data)));
  }

  private void tableMetadata(MsSqlTableMetadataProviderTestData data) throws SQLException {
    assertNotNull(data, "data should not be null.");
    TableMetadataProvider.TableMetadata actual = this.tableMetadataProvider.tableMetadata(new ChangeKey("cdc_testing", "dbo", "users"));
    assertTableMetadata(data.expected(), actual);
  }
}
