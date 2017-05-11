/**
 * Copyright © 2017 Jeremy Custenborder (jcustenborder@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jcustenborder.kafka.connect.cdc;

import com.github.jcustenborder.kafka.connect.utils.config.MarkdownFormatter;
import com.google.common.collect.Maps;
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
