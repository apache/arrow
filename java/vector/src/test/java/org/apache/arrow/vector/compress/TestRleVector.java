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

package org.apache.arrow.vector.compress;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.netty.buffer.ArrowBuf;

/**
 * Test cases for {@link RleVector}.
 */
public class TestRleVector {

  private BufferAllocator allocator;

  @Before
  public void prepare() {
    allocator = new RootAllocator(1024 * 1024);
  }

  @After
  public void shutdown() {
    allocator.close();
  }

  @Test
  public void testAppend() {
    try (VarCharVector rawVector = new VarCharVector("varchar vec", allocator);
    RleVector<VarCharVector> rleVector = new RleVector<>("rle vec", rawVector.getField(), allocator)) {
      rawVector.allocateNew(50, 3);
      rleVector.allocateNew();

      // prepare raw data
      rawVector.set(0, "aaa".getBytes());
      rawVector.set(1, "bbb".getBytes());
      rawVector.set(2, "ccc".getBytes());
      rawVector.setValueCount(3);

      // populate the rle vector
      for (int i = 0; i < 3; i++) {
        rleVector.appendFrom(rawVector, 0);
      }
      for (int i = 0; i < 5; i++) {
        rleVector.appendFrom(rawVector, 1);
      }
      for (int i = 0; i < 10; i++) {
        rleVector.appendFrom(rawVector, 2);
      }

      // verify results
      VarCharVector encoded = rleVector.getEncodedVector();
      assertEquals(3, encoded.getValueCount());

      ArrowBuf idxBuf = rleVector.getRunEndIndexBuffer();
      assertEquals(3, idxBuf.getInt(0));
      assertEquals(8, idxBuf.getInt(RleVector.RUN_LENGTH_BUFFER_WIDTH));
      assertEquals(18, idxBuf.getInt(RleVector.RUN_LENGTH_BUFFER_WIDTH * 2));

      assertEquals("aaa", new String(encoded.get(0)));
      assertEquals("bbb", new String(encoded.get(1)));
      assertEquals("ccc", new String(encoded.get(2)));
    }
  }

  @Test
  public void testTranslateIndex() {
    try (VarCharVector rawVector = new VarCharVector("varchar vec", allocator);
         RleVector<VarCharVector> rleVector = new RleVector<>("rle vec", rawVector.getField(), allocator)) {
      rawVector.allocateNew(50, 3);
      rleVector.allocateNew();

      // prepare raw data
      rawVector.set(0, "aaa".getBytes());
      rawVector.set(1, "bbb".getBytes());
      rawVector.set(2, "ccc".getBytes());
      rawVector.setValueCount(3);

      // populate the rle vector
      for (int i = 0; i < 3; i++) {
        rleVector.appendFrom(rawVector, 0);
      }
      for (int i = 0; i < 5; i++) {
        rleVector.appendFrom(rawVector, 1);
      }
      for (int i = 0; i < 10; i++) {
        rleVector.appendFrom(rawVector, 2);
      }

      // verify results
      assertEquals(0, rleVector.getIndexInEncodedVector(0));
      assertEquals(1, rleVector.getIndexInEncodedVector(3));
      assertEquals(1, rleVector.getIndexInEncodedVector(5));
      assertEquals(2, rleVector.getIndexInEncodedVector(8));
      assertEquals(2, rleVector.getIndexInEncodedVector(12));
      assertEquals(2, rleVector.getIndexInEncodedVector(17));

      assertThrows(IllegalArgumentException.class, () -> {
        assertEquals(2, rleVector.getIndexInEncodedVector(18));
      });
    }
  }

  @Test
  public void testDecode() {
    try (VarCharVector rawVector = new VarCharVector("varchar vec", allocator);
         RleVector<VarCharVector> rleVector = new RleVector<>("rle vec", rawVector.getField(), allocator);
         VarCharVector decodedVector = new VarCharVector("varchar vec", allocator);) {
      rawVector.allocateNew(50, 3);
      rleVector.allocateNew();
      decodedVector.allocateNew();

      // prepare raw data
      rawVector.set(0, "aaa".getBytes());
      rawVector.set(1, "bbb".getBytes());
      rawVector.set(2, "ccc".getBytes());
      rawVector.setValueCount(3);

      // populate the rle vector
      for (int i = 0; i < 1; i++) {
        rleVector.appendFrom(rawVector, 0);
      }
      for (int i = 0; i < 2; i++) {
        rleVector.appendFrom(rawVector, 1);
      }
      for (int i = 0; i < 3; i++) {
        rleVector.appendFrom(rawVector, 2);
      }

      // decode
      rleVector.populateDecodedVector(decodedVector);

      // verify results
      assertEquals(6, decodedVector.getValueCount());

      assertEquals("aaa", new String(decodedVector.get(0)));
      assertEquals("bbb", new String(decodedVector.get(1)));
      assertEquals("bbb", new String(decodedVector.get(2)));
      assertEquals("ccc", new String(decodedVector.get(3)));
      assertEquals("ccc", new String(decodedVector.get(4)));
      assertEquals("ccc", new String(decodedVector.get(5)));
    }
  }
}
