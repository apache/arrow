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
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.holders.NullableDecimalHolder;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration test for a vector with a large (more than 2GB) {@link org.apache.arrow.memory.ArrowBuf} as
 * the data buffer.
 * To run this test, please make sure there is at least 4GB free memory in the system.
 */
public class ITTestLargeVector {
  private static final Logger logger = LoggerFactory.getLogger(ITTestLargeVector.class);

  @Test
  public void testLargeLongVector() {
    logger.trace("Testing large big int vector.");

    final long bufSize = 4 * 1024 * 1024 * 1024L;
    final int vecLength = (int) (bufSize / BigIntVector.TYPE_WIDTH);

    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        BigIntVector largeVec = new BigIntVector("vec", allocator)) {
      largeVec.allocateNew(vecLength);

      logger.trace("Successfully allocated a vector with capacity {}", vecLength);

      for (int i = 0; i < vecLength; i++) {
        largeVec.set(i, i * 10L);

        if ((i + 1) % 10000 == 0) {
          logger.trace("Successfully written {} values", i + 1);
        }
      }
      logger.trace("Successfully written {} values", vecLength);

      for (int i = 0; i < vecLength; i++) {
        long val = largeVec.get(i);
        assertEquals(i * 10L, val);

        if ((i + 1) % 10000 == 0) {
          logger.trace("Successfully read {} values", i + 1);
        }
      }
      logger.trace("Successfully read {} values", vecLength);
    }
    logger.trace("Successfully released the large vector.");
  }

  @Test
  public void testLargeIntVector() {
    logger.trace("Testing large int vector.");

    final long bufSize = 4 * 1024 * 1024 * 1024L;
    final int vecLength = (int) (bufSize / IntVector.TYPE_WIDTH);

    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
         IntVector largeVec = new IntVector("vec", allocator)) {
      largeVec.allocateNew(vecLength);

      logger.trace("Successfully allocated a vector with capacity {}", vecLength);

      for (int i = 0; i < vecLength; i++) {
        largeVec.set(i, i);

        if ((i + 1) % 10000 == 0) {
          logger.trace("Successfully written {} values", i + 1);
        }
      }
      logger.trace("Successfully written {} values", vecLength);

      for (int i = 0; i < vecLength; i++) {
        long val = largeVec.get(i);
        assertEquals(i, val);

        if ((i + 1) % 10000 == 0) {
          logger.trace("Successfully read {} values", i + 1);
        }
      }
      logger.trace("Successfully read {} values", vecLength);
    }
    logger.trace("Successfully released the large vector.");
  }

  @Test
  public void testLargeDecimalVector() {
    logger.trace("Testing large decimal vector.");

    final long bufSize = 4 * 1024 * 1024 * 1024L;
    final int vecLength = (int) (bufSize / DecimalVector.TYPE_WIDTH);

    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
         DecimalVector largeVec = new DecimalVector("vec", allocator, 38, 0)) {
      largeVec.allocateNew(vecLength);

      logger.trace("Successfully allocated a vector with capacity {}", vecLength);

      for (int i = 0; i < vecLength; i++) {
        largeVec.set(i, 0);

        if ((i + 1) % 10000 == 0) {
          logger.trace("Successfully written {} values", i + 1);
        }
      }
      logger.trace("Successfully written {} values", vecLength);

      for (int i = 0; i < vecLength; i++) {
        ArrowBuf buf = largeVec.get(i);
        assertEquals(DecimalVector.TYPE_WIDTH, buf.capacity());
        assertEquals(0, buf.getLong(0));
        assertEquals(0, buf.getLong(8));

        if ((i + 1) % 10000 == 0) {
          logger.trace("Successfully read {} values", i + 1);
        }
      }
      logger.trace("Successfully read {} values", vecLength);

      // try setting values with a large offset in the buffer
      largeVec.set(vecLength - 1, 12345L);
      assertEquals(12345L, largeVec.getObject(vecLength - 1).longValue());

      NullableDecimalHolder holder = new NullableDecimalHolder();
      holder.buffer = largeVec.valueBuffer;
      holder.isSet = 1;
      holder.start = (long) (vecLength - 1) * largeVec.getTypeWidth();
      assertTrue(holder.start > Integer.MAX_VALUE);
      largeVec.set(0, holder);

      BigDecimal decimal = largeVec.getObject(0);
      assertEquals(12345L, decimal.longValue());

      logger.trace("Successfully setting values from large offsets");
    }
    logger.trace("Successfully released the large vector.");
  }

  @Test
  public void testLargeFixedSizeBinaryVector() {
    logger.trace("Testing large fixed size binary vector.");

    final long bufSize = 4 * 1024 * 1024 * 1024L;
    final int typeWidth = 8;
    final int vecLength = (int) (bufSize / typeWidth);

    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
         FixedSizeBinaryVector largeVec = new FixedSizeBinaryVector("vec", allocator, typeWidth)) {
      largeVec.allocateNew(vecLength);

      logger.trace("Successfully allocated a vector with capacity {}", vecLength);

      byte[] value = new byte[] {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'};
      for (int i = 0; i < vecLength; i++) {
        largeVec.set(i, value);

        if ((i + 1) % 10000 == 0) {
          logger.trace("Successfully written {} values", i + 1);
        }
      }
      logger.trace("Successfully written {} values", vecLength);

      for (int i = 0; i < vecLength; i++) {
        byte[] buf = largeVec.get(i);
        assertEquals(typeWidth, buf.length);
        assertArrayEquals(buf, value);

        if ((i + 1) % 10000 == 0) {
          logger.trace("Successfully read {} values", i + 1);
        }
      }
      logger.trace("Successfully read {} values", vecLength);
    }
    logger.trace("Successfully released the large vector.");
  }

  @Test
  public void testLargeVarCharVector() {
    logger.trace("Testing large var char vector.");

    final long bufSize = 4 * 1024 * 1024 * 1024L;
    final int vecLength = (int) (bufSize / BaseVariableWidthVector.OFFSET_WIDTH);
    final String strElement = "a";

    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
         VarCharVector largeVec = new VarCharVector("vec", allocator)) {
      largeVec.allocateNew(vecLength);

      logger.trace("Successfully allocated a vector with capacity " + vecLength);

      for (int i = 0; i < vecLength; i++) {
        largeVec.setSafe(i, strElement.getBytes(StandardCharsets.UTF_8));

        if ((i + 1) % 10000 == 0) {
          logger.trace("Successfully written " + (i + 1) + " values");
        }
      }
      largeVec.setValueCount(vecLength);
      assertTrue(largeVec.getOffsetBuffer().readableBytes() > Integer.MAX_VALUE);
      assertTrue(largeVec.getDataBuffer().readableBytes() < Integer.MAX_VALUE);
      logger.trace("Successfully written " + vecLength + " values");

      for (int i = 0; i < vecLength; i++) {
        byte[] val = largeVec.get(i);
        assertEquals(strElement, new String(val, StandardCharsets.UTF_8));

        if ((i + 1) % 10000 == 0) {
          logger.trace("Successfully read " + (i + 1) + " values");
        }
      }
      logger.trace("Successfully read " + vecLength + " values");
    }
    logger.trace("Successfully released the large vector.");
  }

  @Test
  public void testLargeLargeVarCharVector() {
    logger.trace("Testing large large var char vector.");

    final long bufSize = 4 * 1024 * 1024 * 1024L;
    final int vecLength = (int) (bufSize / BaseLargeVariableWidthVector.OFFSET_WIDTH);
    final String strElement = "9876543210";

    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
         LargeVarCharVector largeVec = new LargeVarCharVector("vec", allocator)) {
      largeVec.allocateNew(vecLength);

      logger.trace("Successfully allocated a vector with capacity " + vecLength);

      for (int i = 0; i < vecLength; i++) {
        largeVec.setSafe(i, strElement.getBytes(StandardCharsets.UTF_8));

        if ((i + 1) % 10000 == 0) {
          logger.trace("Successfully written " + (i + 1) + " values");
        }
      }
      largeVec.setValueCount(vecLength);
      assertTrue(largeVec.getOffsetBuffer().readableBytes() > Integer.MAX_VALUE);
      assertTrue(largeVec.getDataBuffer().readableBytes() > Integer.MAX_VALUE);
      logger.trace("Successfully written " + vecLength + " values");

      for (int i = 0; i < vecLength; i++) {
        byte[] val = largeVec.get(i);
        assertEquals(strElement, new String(val, StandardCharsets.UTF_8));

        if ((i + 1) % 10000 == 0) {
          logger.trace("Successfully read " + (i + 1) + " values");
        }
      }
      logger.trace("Successfully read " + vecLength + " values");
    }
    logger.trace("Successfully released the large vector.");
  }
}
