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

package org.apache.arrow.vector;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.arrow.vector.dictionary.DictionaryEncodeHashMap;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;



public class TestDictionaryEncodeHashMap {

  private List<String> testData = new ArrayList<>();

  private static final int SIZE = 100;

  private static final int KEY_LENGTH = 5;

  private DictionaryEncodeHashMap<String> map = new DictionaryEncodeHashMap();

  @Before
  public void init() {
    for (int i = 0; i < SIZE; i++) {
      testData.add(generateUniqueKey(KEY_LENGTH));
    }
  }

  @After
  public void terminate() throws Exception {
    testData.clear();
  }

  private String generateUniqueKey(int length) {
    String str = "abcdefghijklmnopqrstuvwxyz";
    Random random = new Random();
    StringBuffer sb = new StringBuffer();
    for (int i = 0; i < length; i++) {
      int number = random.nextInt(26);
      sb.append(str.charAt(number));
    }
    if (testData.contains(sb.toString())) {
      return generateUniqueKey(length);
    }
    return sb.toString();
  }

  @Test
  public void testPutAndGet() {
    for (int i = 0; i < SIZE; i++) {
      map.put(testData.get(i), i);
    }

    for (int i = 0; i < SIZE; i++) {
      assertEquals(i, map.get(testData.get(i)));
    }
  }

  @Test
  public void testPutExistKey() {
    for (int i = 0; i < SIZE; i++) {
      map.put(testData.get(i), i);
    }
    map.put("test_key", 101);
    assertEquals(101, map.get("test_key"));
    map.put("test_key", 102);
    assertEquals(102, map.get("test_key"));
  }

  @Test
  public void testGetNonExistKey() {
    for (int i = 0; i < SIZE; i++) {
      map.put(testData.get(i), i);
    }
    //remove if exists.
    map.remove("test_key");
    assertEquals(-1, map.get("test_key"));
  }

  @Test
  public void testRemove() {
    for (int i = 0; i < SIZE; i++) {
      map.put(testData.get(i), i);
    }
    map.put("test_key", 10000);
    assertEquals(SIZE + 1, map.size());
    assertEquals(10000, map.get("test_key"));
    map.remove("test_key");
    assertEquals(SIZE, map.size());
    assertEquals(-1, map.get("test_key"));
  }

  @Test
  public void testSize() {
    assertEquals(0, map.size());
    for (int i = 0; i < SIZE; i++) {
      map.put(testData.get(i), i);
    }
    assertEquals(SIZE, map.size());
  }
}
