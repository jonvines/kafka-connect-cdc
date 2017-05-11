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

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CDCSourceTaskTest {
  CDCSourceTask sourceTask;

  @BeforeEach
  public void before() {
    this.sourceTask = mock(CDCSourceTask.class, Mockito.CALLS_REAL_METHODS);
    this.sourceTask.time = mock(Time.class);

    when(this.sourceTask.getConfig(anyMap())).thenAnswer(new Answer<CDCSourceConnectorConfig>() {
      @Override
      public CDCSourceConnectorConfig answer(InvocationOnMock invocationOnMock) throws Throwable {
        Map<String, String> f = invocationOnMock.getArgument(0);
        return new CDCSourceConnectorConfig(CDCSourceConnectorConfig.config(), f);
      }
    });
    this.sourceTask.start(CDCSourceConnectorConfigTest.settings());
  }

  @Test
  public void pollNoChanges() throws InterruptedException {
    List<SourceRecord> records = this.sourceTask.poll();
    assertNotNull(records);
    assertTrue(records.isEmpty());
  }

  @Test
  public void poll() throws InterruptedException {
    this.sourceTask.addChange(TestData.change());
    this.sourceTask.addChange(TestData.change());
    this.sourceTask.addChange(TestData.change());

    List<SourceRecord> records = this.sourceTask.poll();
    assertNotNull(records, "records should not be null");
    assertFalse(records.isEmpty(), "records should not be empty.");
    assertEquals(3, records.size(), "records.size did not match");

    for (SourceRecord sourceRecord : records) {
      Struct key = (Struct) sourceRecord.key();
      key.validate();
      Struct value = (Struct) sourceRecord.value();
      value.validate();
    }
  }
}
