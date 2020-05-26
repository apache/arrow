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

import org.junit.Assert;
import org.junit.Test;

public class TestMapWithOrdinal {

  @Test
  public void testRemove() {
    MapWithOrdinal<String, String> map = new MapWithOrdinalImpl<>();
    map.put("x", "1", true);
    map.put("y", "2", true);
    map.put("z", "3", true);
    map.removeAll("x");
    Assert.assertEquals(2, map.size());
    Assert.assertEquals(0, map.getOrdinal("z"));
    Assert.assertEquals(1, map.getOrdinal("y"));
    map.put("a", "4", true);
    map.put("b", "5", true);
    map.removeAll("z");
    map.removeAll("a");
    Assert.assertEquals(2, map.size());
    Assert.assertEquals("2", map.get("y"));
    Assert.assertEquals("5", map.get("b"));
    Assert.assertNull(map.get("x"));
    Assert.assertNull(map.get("z"));
    Assert.assertNull(map.get("a"));
    Assert.assertEquals(1, map.getOrdinal("y"));
    Assert.assertEquals(0, map.getOrdinal("b"));
  }
}
