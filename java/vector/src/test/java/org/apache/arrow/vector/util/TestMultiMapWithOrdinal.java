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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class TestMultiMapWithOrdinal {

  @Test
  public void test() {
    MultiMapWithOrdinal<String, String> map = new MultiMapWithOrdinal<>();

    map.put("x", "1", false);
    assertEquals(1, map.size());
    map.remove("x", "1");
    assertTrue(map.isEmpty());
    map.put("x", "1", false);
    map.put("x", "2", false);
    map.put("y", "0", false);
    assertEquals(3, map.size());
    assertEquals(2, map.getAll("x").size());
    assertEquals("1", map.getAll("x").stream().findFirst().get());
    assertEquals("1", map.getByOrdinal(0));
    assertEquals("2", map.getByOrdinal(1));
    assertEquals("0", map.getByOrdinal(2));
    assertTrue(map.remove("x", "1"));
    assertFalse(map.remove("x", "1"));
    assertEquals("0", map.getByOrdinal(0));
    assertEquals(2, map.size());
    map.put("x", "3", true);
    assertEquals(1, map.getAll("x").size());
    assertEquals("3", map.getAll("x").stream().findFirst().get());
    map.put("z", "4", false);
    assertEquals(3, map.size());
    map.put("z", "5", false);
    map.put("z", "6", false);
    assertEquals(5, map.size());
    map.removeAll("z");
    assertEquals(2, map.size());
    assertFalse(map.containsKey("z"));
  }
}
