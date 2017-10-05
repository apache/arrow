/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.arrow.memory;

import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Test;

/**
 * To test simplified implementation of IdentityHashMap
 */
public class TestLowCostIdentityHasMap {

  @Test
  public void testIdentityHashMap() throws Exception {
    LowCostIdentityHasMap<String, StringWithKey> hashMap = new LowCostIdentityHasMap<>();

    StringWithKey obj1 = new StringWithKey("s1key", "s1value");
    StringWithKey obj2 = new StringWithKey("s2key", "s2value");
    StringWithKey obj3 = new StringWithKey("s3key", "s3value");
    StringWithKey obj4 = new StringWithKey("s1key", "s4value");
    StringWithKey obj5 = new StringWithKey("s5key", "s5value");

    assertNull(hashMap.put(obj1));
    assertNull(hashMap.put(obj2));
    assertNull(hashMap.put(obj3));
    assertEquals(obj1, hashMap.put(obj4));
    assertNull(hashMap.put(obj5));

    assertEquals(4, hashMap.size());

    assertEquals(obj4,hashMap.get("s1key"));

    assertNull(hashMap.remove("abc"));

    assertEquals(obj3,hashMap.remove("s3key"));

    assertEquals(3, hashMap.size());

    assertTrue(!hashMap.isEmpty());

    StringWithKey nextValue = hashMap.getNextValue();

    assertNotNull(nextValue);

    assertTrue((hashMap.get("s1key") == nextValue || hashMap.get("s2key") == nextValue ||
      hashMap.get("s5key") == nextValue));

    assertTrue(hashMap.containsValue(obj4));
    assertTrue(hashMap.containsValue(obj2));
    assertTrue(hashMap.containsValue(obj5));

    assertEquals(obj4,hashMap.remove("s1key"));

    nextValue = hashMap.getNextValue();

    assertNotNull(nextValue);

    assertTrue(hashMap.get("s2key") == nextValue || hashMap.get("s5key") == nextValue);

    assertEquals(2, hashMap.size());

    assertEquals(obj2,hashMap.remove("s2key"));
    assertEquals(obj5,hashMap.remove("s5key"));

    assertEquals(0, hashMap.size());

    assertTrue(hashMap.isEmpty());
  }

  @Test
  public void testLargeMap() throws Exception {
    LowCostIdentityHasMap<String, StringWithKey> hashMap = new LowCostIdentityHasMap<>();

    String [] keys = new String[200];
    for (int i = 0; i < 200; i++) {
      keys[i] = "s"+i+"key";
    }

    for (int i = 0; i < 100; i++) {
      if (i % 5 == 0 && i != 0) {
        StringWithKey obj = new StringWithKey(keys[i-5], "s" + i + "value");
        StringWithKey retObj = hashMap.put(obj);
        assertNotNull(retObj);
        StringWithKey obj1 = new StringWithKey(keys[i], "s" + 2*i + "value");
        StringWithKey retObj1 = hashMap.put(obj1);
        assertNull(retObj1);
      } else {
        StringWithKey obj = new StringWithKey(keys[i], "s" + i + "value");
        StringWithKey retObj = hashMap.put(obj);
        assertNull(retObj);
      }
    }
    assertEquals(100, hashMap.size());
    for (int i = 0; i < 100; i++) {
      StringWithKey returnObj = hashMap.get(keys[i]);
      assertNotNull(returnObj);
      if (i == 95) {
        assertEquals("s190value", returnObj.getValue());
        continue;
      }
      if (i % 5 == 0) {
        assertEquals("s" + (i+5) + "value", returnObj.getValue());
      } else {
        assertEquals("s" + i + "value", returnObj.getValue());
      }
    }

    for (int i = 0; i < 100; i++) {
      if (i % 4 == 0) {
        StringWithKey returnObj = hashMap.remove(keys[i]);
        assertNotNull(returnObj);
        assertTrue(!hashMap.containsKey(keys[i]));
      }
      StringWithKey obj = new StringWithKey(keys[100+i], "s" + (100+i) + "value");
      StringWithKey retObj = hashMap.put(obj);
      assertNull(retObj);
      assertTrue(hashMap.containsKey(keys[100+i]));
    }
    assertEquals(175, hashMap.size());
    for (int i = 0; i < 100; i++) {
      StringWithKey retObj = hashMap.getNextValue();
      assertNotNull(retObj);
      hashMap.remove(retObj.getKey());
    }
    assertTrue(!hashMap.isEmpty());
    assertEquals(75, hashMap.size());
    hashMap.clear();
    assertTrue(hashMap.isEmpty());
  }

  private class StringWithKey implements ValueWithKeyIncluded<String> {

    private String myValue;
    private String myKey;

    StringWithKey(String myKey, String myValue) {
      this.myKey = myKey;
      this.myValue = myValue;
    }

    @Override
    public String getKey() {
      return myKey;
    }

    String getValue() {
      return myValue;
    }
  }
}
