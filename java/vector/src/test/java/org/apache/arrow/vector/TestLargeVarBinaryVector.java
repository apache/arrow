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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.holders.NullableLargeVarBinaryHolder;
import org.apache.arrow.vector.util.ReusableByteArray;
import org.apache.arrow.vector.util.TransferPair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestLargeVarBinaryVector {

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
  public void testSetNullableLargeVarBinaryHolder() {
    try (LargeVarBinaryVector vector = new LargeVarBinaryVector("", allocator)) {
      vector.allocateNew(100, 10);

      NullableLargeVarBinaryHolder nullHolder = new NullableLargeVarBinaryHolder();
      nullHolder.isSet = 0;

      NullableLargeVarBinaryHolder binHolder = new NullableLargeVarBinaryHolder();
      binHolder.isSet = 1;

      String str = "hello";
      try (ArrowBuf buf = allocator.buffer(16)) {
        buf.setBytes(0, str.getBytes(StandardCharsets.UTF_8));

        binHolder.start = 0;
        binHolder.end = str.length();
        binHolder.buffer = buf;

        vector.set(0, nullHolder);
        vector.set(1, binHolder);

        // verify results
        assertTrue(vector.isNull(0));
        assertEquals(str, new String(Objects.requireNonNull(vector.get(1)), StandardCharsets.UTF_8));
      }
    }
  }

  @Test
  public void testSetNullableLargeVarBinaryHolderSafe() {
    try (LargeVarBinaryVector vector = new LargeVarBinaryVector("", allocator)) {
      vector.allocateNew(5, 1);

      NullableLargeVarBinaryHolder nullHolder = new NullableLargeVarBinaryHolder();
      nullHolder.isSet = 0;

      NullableLargeVarBinaryHolder binHolder = new NullableLargeVarBinaryHolder();
      binHolder.isSet = 1;

      String str = "hello world";
      try (ArrowBuf buf = allocator.buffer(16)) {
        buf.setBytes(0, str.getBytes(StandardCharsets.UTF_8));

        binHolder.start = 0;
        binHolder.end = str.length();
        binHolder.buffer = buf;

        vector.setSafe(0, binHolder);
        vector.setSafe(1, nullHolder);

        // verify results
        assertEquals(str, new String(Objects.requireNonNull(vector.get(0)), StandardCharsets.UTF_8));
        assertTrue(vector.isNull(1));
      }
    }
  }

  @Test
  public void testGetBytesRepeatedly() {
    try (LargeVarBinaryVector vector = new LargeVarBinaryVector("", allocator)) {
      vector.allocateNew(5, 1);

      final String str = "hello world";
      final String str2 = "foo";
      vector.setSafe(0, str.getBytes(StandardCharsets.UTF_8));
      vector.setSafe(1, str2.getBytes(StandardCharsets.UTF_8));

      // verify results
      ReusableByteArray reusableByteArray = new ReusableByteArray();
      vector.read(0, reusableByteArray);
      byte[] oldBuffer = reusableByteArray.getBuffer();
      assertArrayEquals(str.getBytes(StandardCharsets.UTF_8), Arrays.copyOfRange(reusableByteArray.getBuffer(),
          0, (int) reusableByteArray.getLength()));

      vector.read(1, reusableByteArray);
      assertArrayEquals(str2.getBytes(StandardCharsets.UTF_8), Arrays.copyOfRange(reusableByteArray.getBuffer(),
          0, (int) reusableByteArray.getLength()));

      // There should not have been any reallocation since the newer value is smaller in length.
      assertSame(oldBuffer, reusableByteArray.getBuffer());
    }
  }

  @Test
  public void testGetTransferPairWithField() {
    try (BufferAllocator childAllocator1 = allocator.newChildAllocator("child1", 1000000, 1000000);
        LargeVarBinaryVector v1 = new LargeVarBinaryVector("v1", childAllocator1)) {
      v1.allocateNew();
      v1.setSafe(4094, "hello world".getBytes(StandardCharsets.UTF_8), 0, 11);
      v1.setValueCount(4001);

      TransferPair tp = v1.getTransferPair(v1.getField(), allocator);
      tp.transfer();
      LargeVarBinaryVector v2 = (LargeVarBinaryVector) tp.getTo();
      assertSame(v1.getField(), v2.getField());
      v2.clear();
    }
  }
}
