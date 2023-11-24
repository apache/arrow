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

package org.apache.arrow.memory.netty;

import static org.junit.Assert.assertEquals;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Integration test for large (more than 2GB) {@link org.apache.arrow.memory.ArrowBuf}.
 * To run this test, please make sure there is at least 4GB memory in the system.
 */
public class ITTestLargeArrowBuf {
  private static final Logger logger = LoggerFactory.getLogger(ITTestLargeArrowBuf.class);

  private void run(long bufSize) {
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
         ArrowBuf largeBuf = allocator.buffer(bufSize)) {
      assertEquals(bufSize, largeBuf.capacity());
      logger.trace("Successfully allocated a buffer with capacity {}", largeBuf.capacity());

      for (long i = 0; i < bufSize / 8; i++) {
        largeBuf.setLong(i * 8, i);

        if ((i + 1) % 10000 == 0) {
          logger.trace("Successfully written {} long words", i + 1);
        }
      }
      logger.trace("Successfully written {} long words", bufSize / 8);

      for (long i = 0; i < bufSize / 8; i++) {
        long val = largeBuf.getLong(i * 8);
        assertEquals(i, val);

        if ((i + 1) % 10000 == 0) {
          logger.trace("Successfully read {} long words", i + 1);
        }
      }
      logger.trace("Successfully read {} long words", bufSize / 8);
    }
    logger.trace("Successfully released the large buffer.");
  }

  @Test
  public void testLargeArrowBuf() {
    run(4 * 1024 * 1024 * 1024L);
  }

  @Test
  public void testMaxIntArrowBuf() {
    run(Integer.MAX_VALUE);
  }

}
