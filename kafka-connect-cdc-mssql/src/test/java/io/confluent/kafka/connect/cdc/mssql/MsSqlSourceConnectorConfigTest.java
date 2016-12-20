package io.confluent.kafka.connect.cdc.mssql;

import io.confluent.kafka.connect.utils.config.MarkdownFormatter;
import org.junit.jupiter.api.Test;

public class MsSqlSourceConnectorConfigTest {
  @Test
  public void doc() {
    System.out.println(MarkdownFormatter.toMarkdown(MsSqlSourceConnectorConfig.config()));
  }

}
