package io.confluent.kafka.connect.cdc;

import com.google.common.collect.Maps;
import io.confluent.kafka.connect.utils.config.MarkdownFormatter;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class CDCSourceConnectorConfigTest {

  public static Map<String, String> settings() {
    Map<String, String> settings = Maps.newLinkedHashMap();
    settings.put(CDCSourceConnectorConfig.SCHEMA_CASE_FORMAT_DATABASE_NAMES_CONFIG, CDCSourceConnectorConfig.CaseFormat.LOWER.name());
    settings.put(CDCSourceConnectorConfig.SCHEMA_CASE_FORMAT_TABLE_NAMES_CONFIG, CDCSourceConnectorConfig.CaseFormat.UPPER_CAMEL.name());
    settings.put(CDCSourceConnectorConfig.SCHEMA_CASE_FORMAT_COLUMN_NAMES_CONFIG, CDCSourceConnectorConfig.CaseFormat.LOWER_CAMEL.name());

    return settings;
  }

  @Test
  public void markdown() {
    System.out.println(MarkdownFormatter.toMarkdown(CDCSourceConnectorConfig.config()));
  }

}
