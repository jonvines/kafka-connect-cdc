package io.confluent.kafka.connect.cdc;

import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.connect.utils.config.MarkdownFormatter;
import org.junit.Test;

import java.util.Map;

public class CDCSourceConnectorConfigTest {

  public static Map<String, String> settings() {
    return ImmutableMap.of(
        CDCSourceConnectorConfig.NAMESPACE_CONFIG, "com.example.cdc.${sourceDatabaseName}",
        CDCSourceConnectorConfig.KEY_NAME_FORMAT_CONFIG, "${namespace}.${tableName}Key",
        CDCSourceConnectorConfig.VALUE_NAME_FORMAT_CONFIG, "${namespace}.${tableName}Value"
    );
  }

  @Test
  public void markdown() {
    System.out.println(MarkdownFormatter.toMarkdown(CDCSourceConnectorConfig.config()));
  }

}
