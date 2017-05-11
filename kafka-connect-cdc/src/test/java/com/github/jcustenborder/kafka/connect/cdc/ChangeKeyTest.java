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

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
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
