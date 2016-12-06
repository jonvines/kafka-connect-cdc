package io.confluent.kafka.connect.cdc;

import com.google.common.base.CaseFormat;
import com.google.common.collect.ImmutableMap;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.*;

public class CDCSourceTaskTest {
  CDCSourceTask sourceTask;

  @Before
  public void before() {
    this.sourceTask = mock(CDCSourceTask.class, Mockito.CALLS_REAL_METHODS);
    this.sourceTask.time = mock(Time.class);

    when(this.sourceTask.getConfig(anyMap())).thenAnswer(new Answer<CDCSourceConnectorConfig>() {
      @Override
      public CDCSourceConnectorConfig answer(InvocationOnMock invocationOnMock) throws Throwable {
        Map<String, String> f = invocationOnMock.getArgumentAt(0, Map.class);
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
    verify(this.sourceTask.time, only()).sleep(anyLong());
  }

  @Test
  public void poll() throws InterruptedException {
    this.sourceTask.addChange(TestData.change());
    this.sourceTask.addChange(TestData.change());
    this.sourceTask.addChange(TestData.change());

    List<SourceRecord> records = this.sourceTask.poll();
    assertNotNull("records should not be null", records);
    assertFalse("records should not be empty.", records.isEmpty());
    assertEquals("records.size did not match", 3, records.size());

    for(SourceRecord sourceRecord: records) {
      Struct key = (Struct) sourceRecord.key();
      key.validate();
      Struct value = (Struct) sourceRecord.value();
      value.validate();
    }
  }
}
