package io.confluent.kafka.connect.cdc.mssql;

import io.confluent.kafka.connect.utils.config.MarkdownFormatter;
import org.junit.jupiter.api.Test;

public class MsSQLSourceConnectorConfigTest {
  @Test
  public void doc() {
    System.out.println(MarkdownFormatter.toMarkdown(MsSQLSourceConnectorConfig.config()));
  }
}
