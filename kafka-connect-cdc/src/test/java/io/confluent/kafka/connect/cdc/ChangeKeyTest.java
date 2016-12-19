package io.confluent.kafka.connect.cdc;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ChangeKeyTest {
  @Test
  public void equals() {
    Change change = mock(Change.class);
    when(change.schemaName()).thenReturn("SourceDatabase");
    when(change.tableName()).thenReturn("ObjectOwner");

    final ChangeKey changeKeyThis = new ChangeKey(change);
    final ChangeKey changeKeyThat = new ChangeKey(change);

    assertEquals(changeKeyThis, changeKeyThat);
    assertNotEquals(changeKeyThis, new Object());

  }

  @Test
  public void compareTo() {
    Change change = mock(Change.class);
    when(change.schemaName()).thenReturn("SourceDatabase");
    when(change.tableName()).thenReturn("ObjectOwner");

    final ChangeKey changeKeyThis = new ChangeKey(change);
    final ChangeKey changeKeyThat = new ChangeKey(change);

    assertEquals(0, changeKeyThis.compareTo(changeKeyThat));
  }

  @Test
  public void mapKey() {
    String EXPECTED_VALUE = "this is a test value.";
    Map<ChangeKey, String> map = new HashMap<>();

    Change change = mock(Change.class);
    when(change.schemaName()).thenReturn("SourceDatabase");
    when(change.tableName()).thenReturn("ObjectOwner");

    final ChangeKey changeKeyThis = new ChangeKey(change);
    map.put(changeKeyThis, EXPECTED_VALUE);
    assertEquals(EXPECTED_VALUE, map.get(changeKeyThis));
  }

  @Test
  public void tostring() {
    Change change = mock(Change.class);
    when(change.schemaName()).thenReturn("SourceDatabase");
    when(change.tableName()).thenReturn("ObjectOwner");

    final ChangeKey changeKeyThis = new ChangeKey(change);
    System.out.println(changeKeyThis);
  }

}
