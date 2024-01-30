/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.vector.util;

import static junit.framework.TestCase.assertNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collection;

import org.junit.Before;
import org.junit.Test;

public class TestMapWithOrdinal {

  private MapWithOrdinal<String, String> map;

  @Before
  public void setUp() {
    map = new MapWithOrdinalImpl<>();
  }

  @Test
  public void testGetByOrdinal() {
    map.put("key0", "val0", true);
    assertEquals("val0", map.getByOrdinal(0));

    map.put("key1", "val1", true);
    assertEquals("val1", map.getByOrdinal(1));
    assertEquals("val0", map.getByOrdinal(0));
  }

  @Test
  public void testGetByKey() {
    map.put("key0", "val0", true);
    assertEquals("val0", map.get("key0"));

    map.put("key1", "val1", true);
    assertEquals("val1", map.get("key1"));
    assertEquals("val0", map.get("key0"));
  }

  @Test
  public void testInvalidOrdinal() {
    map.put("key0", "val0", true);
    assertNull(map.getByOrdinal(1));

    map.removeAll("key0");
    assertNull(map.getByOrdinal(0));
  }

  @Test
  public void testInvalidKey() {
    MapWithOrdinalImpl<String, String> map = new MapWithOrdinalImpl<>();
    map.put("key0", "val0", true);
    assertNull(map.get("fake_key"));

    map.removeAll("key0");
    assertNull(map.get("key0"));
  }

  @Test
  public void testValues() {
    map.put("key0", "val0", true);
    map.put("key1", "val1", true);

    Collection<String> values = map.values();
    assertTrue(values.contains("val0"));
    assertTrue(values.contains("val1"));

    map.put("key1", "new_val1", true);
    values = map.values();
    assertTrue(values.contains("val0"));
    assertTrue(values.contains("new_val1"));
    assertFalse(values.contains("val1"));

    map.removeAll("key0");
    assertTrue(values.contains("new_val1"));
    assertFalse(values.contains("val0"));
  }
}
