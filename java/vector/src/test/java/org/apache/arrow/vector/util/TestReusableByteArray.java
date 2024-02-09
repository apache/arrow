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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BaseValueVector;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestReusableByteArray {

  private BufferAllocator allocator;

  @Before
  public void prepare() {
    // Permit allocating 4 vectors of max size.
    allocator = new RootAllocator(4 * BaseValueVector.MAX_ALLOCATION_SIZE);
  }

  @After
  public void shutdown() {
    allocator.close();
  }

  @Test
  public void testSetByteArrayRepeatedly() {
    ReusableByteArray byteArray = new ReusableByteArray();
    try (ArrowBuf workingBuf = allocator.buffer(100)) {
      final String str = "test";
      workingBuf.setBytes(0, str.getBytes(StandardCharsets.UTF_8));
      byteArray.set(workingBuf, 0, str.getBytes(StandardCharsets.UTF_8).length);
      assertEquals(str.getBytes(StandardCharsets.UTF_8).length, byteArray.getLength());
      assertArrayEquals(str.getBytes(StandardCharsets.UTF_8), Arrays.copyOfRange(byteArray.getBuffer(), 0,
          (int) byteArray.getLength()));
      assertEquals(Base64.getEncoder().encodeToString(str.getBytes(StandardCharsets.UTF_8)), byteArray.toString());
      assertEquals(new ReusableByteArray(str.getBytes(StandardCharsets.UTF_8)), byteArray);
      assertEquals(new ReusableByteArray(str.getBytes(StandardCharsets.UTF_8)).hashCode(), byteArray.hashCode());

      // Test a longer string. Should require reallocation.
      final String str2 = "test_longer";
      byte[] oldBuffer = byteArray.getBuffer();
      workingBuf.clear();
      workingBuf.setBytes(0, str2.getBytes(StandardCharsets.UTF_8));
      byteArray.set(workingBuf, 0, str2.getBytes(StandardCharsets.UTF_8).length);
      assertEquals(str2.getBytes(StandardCharsets.UTF_8).length, byteArray.getLength());
      assertArrayEquals(str2.getBytes(StandardCharsets.UTF_8), Arrays.copyOfRange(byteArray.getBuffer(), 0,
          (int) byteArray.getLength()));
      assertEquals(Base64.getEncoder().encodeToString(str2.getBytes(StandardCharsets.UTF_8)), byteArray.toString());
      assertEquals(new ReusableByteArray(str2.getBytes(StandardCharsets.UTF_8)), byteArray);
      assertEquals(new ReusableByteArray(str2.getBytes(StandardCharsets.UTF_8)).hashCode(), byteArray.hashCode());

      // Verify reallocation needed.
      assertNotSame(oldBuffer, byteArray.getBuffer());
      assertTrue(byteArray.getBuffer().length > oldBuffer.length);

      // Test writing a shorter string. Should not require reallocation.
      final String str3 = "short";
      oldBuffer = byteArray.getBuffer();
      workingBuf.clear();
      workingBuf.setBytes(0, str3.getBytes(StandardCharsets.UTF_8));
      byteArray.set(workingBuf, 0, str3.getBytes(StandardCharsets.UTF_8).length);
      assertEquals(str3.getBytes(StandardCharsets.UTF_8).length, byteArray.getLength());
      assertArrayEquals(str3.getBytes(StandardCharsets.UTF_8), Arrays.copyOfRange(byteArray.getBuffer(), 0,
          (int) byteArray.getLength()));
      assertEquals(Base64.getEncoder().encodeToString(str3.getBytes(StandardCharsets.UTF_8)), byteArray.toString());
      assertEquals(new ReusableByteArray(str3.getBytes(StandardCharsets.UTF_8)), byteArray);
      assertEquals(new ReusableByteArray(str3.getBytes(StandardCharsets.UTF_8)).hashCode(), byteArray.hashCode());

      // Verify reallocation was not needed.
      assertSame(oldBuffer, byteArray.getBuffer());
    }
  }
}
