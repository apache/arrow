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


import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Random;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class TestVarCharViewVector {

  private static final byte[] STR0 = "0123456".getBytes(StandardCharsets.UTF_8);
  private static final byte[] STR1 = "012345678912".getBytes(StandardCharsets.UTF_8);
  private static final byte[] STR2 = "0123456789123".getBytes(StandardCharsets.UTF_8);
  private static final byte[] STR3 = "01234567891234567".getBytes(StandardCharsets.UTF_8);
  private static final byte[] STR4 = "01234567".getBytes(StandardCharsets.UTF_8);
  private static final byte[] STR5 = "012345678912345678".getBytes(StandardCharsets.UTF_8);
  private static final byte[] STR6 = "01234567891234567890".getBytes(StandardCharsets.UTF_8);

  private BufferAllocator allocator;

  @Before
  public void prepare() {
    allocator = new RootAllocator(Integer.MAX_VALUE);
  }

  @After
  public void shutdown() {
    allocator.close();
  }

  @Test
  public void testInlineAllocation() {
    try (final ViewVarCharVector viewVarCharVector = new ViewVarCharVector("myvector", allocator)) {
      viewVarCharVector.allocateNew(48, 3);
      final int valueCount = 3;
      viewVarCharVector.set(0, STR0);
      viewVarCharVector.set(1, STR1);
      viewVarCharVector.set(2, STR4);
      viewVarCharVector.setValueCount(valueCount);

      byte[] view1 = viewVarCharVector.get(0);
      byte[] view2 = viewVarCharVector.get(1);
      byte[] view3 = viewVarCharVector.get(2);

      assert view1 != null;
      assert view2 != null;
      assert view3 != null;

      String str1 = new String(STR0, StandardCharsets.UTF_8);
      String str2 = new String(STR1, StandardCharsets.UTF_8);
      String str3 = new String(STR4, StandardCharsets.UTF_8);

      Assert.assertEquals(new String(view1, StandardCharsets.UTF_8), str1);
      Assert.assertEquals(new String(view2, StandardCharsets.UTF_8), str2);
      Assert.assertEquals(new String(view3, StandardCharsets.UTF_8), str3);
      
      assert viewVarCharVector.dataBuffers.isEmpty();
      
      Assert.assertEquals(new String(Objects.requireNonNull(viewVarCharVector.getObject(0)).getBuffer(),
          StandardCharsets.UTF_8), str1);
      Assert.assertEquals(new String(Objects.requireNonNull(viewVarCharVector.getObject(1)).getBuffer(),
          StandardCharsets.UTF_8), str2);
      Assert.assertEquals(new String(Objects.requireNonNull(viewVarCharVector.getObject(2)).getBuffer(),
          StandardCharsets.UTF_8), str3);
    }
  }

  @Test
  public void testReferenceAllocationInSameBuffer() {
    try (final ViewVarCharVector viewVarCharVector = new ViewVarCharVector("myvector", allocator)) {
      viewVarCharVector.allocateNew(48, 4);
      final int valueCount = 4;
      String str4 = generateRandomString(34);
      viewVarCharVector.set(0, STR1);
      viewVarCharVector.set(1, STR2);
      viewVarCharVector.set(2, STR3);
      viewVarCharVector.set(3, str4.getBytes(StandardCharsets.UTF_8));
      viewVarCharVector.setValueCount(valueCount);

      byte[] view1 = viewVarCharVector.get(0);
      byte[] view2 = viewVarCharVector.get(1);
      byte[] view3 = viewVarCharVector.get(2);
      byte[] view4 = viewVarCharVector.get(3);

      assert view1 != null;
      assert view2 != null;
      assert view3 != null;
      assert view4 != null;

      String str1 = new String(STR1, StandardCharsets.UTF_8);
      String str2 = new String(STR2, StandardCharsets.UTF_8);
      String str3 = new String(STR3, StandardCharsets.UTF_8);

      Assert.assertEquals(new String(view1, StandardCharsets.UTF_8), str1);
      Assert.assertEquals(new String(view2, StandardCharsets.UTF_8), str2);
      Assert.assertEquals(new String(view3, StandardCharsets.UTF_8), str3);
      Assert.assertEquals(new String(view4, StandardCharsets.UTF_8), str4);

      assert viewVarCharVector.dataBuffers.size() == 1;

      Assert.assertEquals(new String(Objects.requireNonNull(viewVarCharVector.getObject(0)).getBuffer(),
          StandardCharsets.UTF_8), str1);
      Assert.assertEquals(new String(Objects.requireNonNull(viewVarCharVector.getObject(1)).getBuffer(),
          StandardCharsets.UTF_8), str2);
      Assert.assertEquals(new String(Objects.requireNonNull(viewVarCharVector.getObject(2)).getBuffer(),
          StandardCharsets.UTF_8), str3);
      Assert.assertEquals(new String(Objects.requireNonNull(viewVarCharVector.getObject(3)).getBuffer(),
          StandardCharsets.UTF_8), str4);
    }
  }

  @Test
  public void testReferenceAllocationInOtherBuffer() {
    try (final ViewVarCharVector viewVarCharVector = new ViewVarCharVector("myvector", allocator)) {
      viewVarCharVector.allocateNew(48, 4);
      final int valueCount = 4;
      String str4 = generateRandomString(35);
      viewVarCharVector.set(0, STR1);
      viewVarCharVector.set(1, STR2);
      viewVarCharVector.set(2, STR3);
      viewVarCharVector.set(3, str4.getBytes(StandardCharsets.UTF_8));
      viewVarCharVector.setValueCount(valueCount);

      byte[] view1 = viewVarCharVector.get(0);
      byte[] view2 = viewVarCharVector.get(1);
      byte[] view3 = viewVarCharVector.get(2);
      byte[] view4 = viewVarCharVector.get(3);

      assert view1 != null;
      assert view2 != null;
      assert view3 != null;
      assert view4 != null;

      String str1 = new String(STR1, StandardCharsets.UTF_8);
      String str2 = new String(STR2, StandardCharsets.UTF_8);
      String str3 = new String(STR3, StandardCharsets.UTF_8);

      Assert.assertEquals(new String(view1, StandardCharsets.UTF_8), str1);
      Assert.assertEquals(new String(view2, StandardCharsets.UTF_8), str2);
      Assert.assertEquals(new String(view3, StandardCharsets.UTF_8), str3);
      Assert.assertEquals(new String(view4, StandardCharsets.UTF_8), str4);

      assert viewVarCharVector.dataBuffers.size() == 2;

      Assert.assertEquals(new String(Objects.requireNonNull(viewVarCharVector.getObject(0)).getBuffer(),
          StandardCharsets.UTF_8), str1);
      Assert.assertEquals(new String(Objects.requireNonNull(viewVarCharVector.getObject(1)).getBuffer(),
          StandardCharsets.UTF_8), str2);
      Assert.assertEquals(new String(Objects.requireNonNull(viewVarCharVector.getObject(2)).getBuffer(),
          StandardCharsets.UTF_8), str3);
      Assert.assertEquals(new String(Objects.requireNonNull(viewVarCharVector.getObject(3)).getBuffer(),
          StandardCharsets.UTF_8), str4);
    }
  }

  @Test
  public void testMixedAllocation() {
    try (final ViewVarCharVector viewVarCharVector = new ViewVarCharVector("myvector", allocator)) {
      viewVarCharVector.allocateNew(128, 6);
      final int valueCount = 6;
      String str4 = generateRandomString(35);
      String str6 = generateRandomString(40);
      viewVarCharVector.set(0, STR1);
      viewVarCharVector.set(1, STR2);
      viewVarCharVector.set(2, STR3);
      viewVarCharVector.set(3, str4.getBytes(StandardCharsets.UTF_8));
      viewVarCharVector.set(4, STR1);
      viewVarCharVector.set(5, str6.getBytes(StandardCharsets.UTF_8));
      viewVarCharVector.setValueCount(valueCount);

      byte[] view1 = viewVarCharVector.get(0);
      byte[] view2 = viewVarCharVector.get(1);
      byte[] view3 = viewVarCharVector.get(2);
      byte[] view4 = viewVarCharVector.get(3);
      byte[] view5 = viewVarCharVector.get(4);
      byte[] view6 = viewVarCharVector.get(5);

      assert view1 != null;
      assert view2 != null;
      assert view3 != null;
      assert view4 != null;
      assert view5 != null;
      assert view6 != null;

      String str1 = new String(STR1, StandardCharsets.UTF_8);
      String str2 = new String(STR2, StandardCharsets.UTF_8);
      String str3 = new String(STR3, StandardCharsets.UTF_8);

      Assert.assertEquals(new String(view1, StandardCharsets.UTF_8), str1);
      Assert.assertEquals(new String(view2, StandardCharsets.UTF_8), str2);
      Assert.assertEquals(new String(view3, StandardCharsets.UTF_8), str3);
      Assert.assertEquals(new String(view4, StandardCharsets.UTF_8), str4);
      Assert.assertEquals(new String(view5, StandardCharsets.UTF_8), str1);
      Assert.assertEquals(new String(view6, StandardCharsets.UTF_8), str6);

      assert viewVarCharVector.dataBuffers.size() == 1;

      Assert.assertEquals(new String(Objects.requireNonNull(viewVarCharVector.getObject(0)).getBuffer(),
          StandardCharsets.UTF_8), str1);
      Assert.assertEquals(new String(Objects.requireNonNull(viewVarCharVector.getObject(1)).getBuffer(),
          StandardCharsets.UTF_8), str2);
      Assert.assertEquals(new String(Objects.requireNonNull(viewVarCharVector.getObject(2)).getBuffer(),
          StandardCharsets.UTF_8), str3);
      Assert.assertEquals(new String(Objects.requireNonNull(viewVarCharVector.getObject(3)).getBuffer(),
          StandardCharsets.UTF_8), str4);
      Assert.assertEquals(new String(Objects.requireNonNull(viewVarCharVector.getObject(4)).getBuffer(),
          StandardCharsets.UTF_8), str1);
      Assert.assertEquals(new String(Objects.requireNonNull(viewVarCharVector.getObject(5)).getBuffer(),
          StandardCharsets.UTF_8), str6);
    }
  }

  @Test(expected = IndexOutOfBoundsException.class)
  public void testAllocationIndexOutOfBounds() {
    try (final ViewVarCharVector viewVarCharVector = new ViewVarCharVector("myvector", allocator)) {
      viewVarCharVector.allocateNew(32, 3);
      final int valueCount = 3;
      viewVarCharVector.set(0, STR1);
      viewVarCharVector.set(1, STR2);
      viewVarCharVector.set(2, STR2);
      viewVarCharVector.setValueCount(valueCount);
    }
  }

  private String generateRandomString(int length) {
    Random random = new Random();
    StringBuilder sb = new StringBuilder(length);
    for (int i = 0; i < length; i++) {
      sb.append(random.nextInt(10)); // 0-9
    }
    return sb.toString();
  }
}
