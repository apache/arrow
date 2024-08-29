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

import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.stream.Collectors;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.ReferenceManager;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.netty.buffer.PooledByteBufAllocatorL;

/**
 * Test netty allocators.
 */
public class TestNettyAllocator {

  @Test
  public void testMemoryUsage() {
    ListAppender<ILoggingEvent> memoryLogsAppender = new ListAppender<>();
    memoryLogsAppender.list = Collections.synchronizedList(memoryLogsAppender.list);
    Logger logger = (Logger) LoggerFactory.getLogger("arrow.allocator");
    try {
      logger.setLevel(Level.TRACE);
      logger.addAppender(memoryLogsAppender);
      memoryLogsAppender.start();
      try (ArrowBuf buf = new ArrowBuf(ReferenceManager.NO_OP, null,
          1024, new PooledByteBufAllocatorL().empty.memoryAddress())) {
        buf.memoryAddress();
      }
      boolean result = false;
      long startTime = System.currentTimeMillis();
      while ((System.currentTimeMillis() - startTime) < 10000) { // 10 seconds maximum for time to read logs
        // Lock on the list backing the appender since a background thread might try to add more logs
        // while stream() is iterating over list elements. This would throw a flakey ConcurrentModificationException.
        synchronized (memoryLogsAppender.list) {
          result = memoryLogsAppender.list.stream()
              .anyMatch(
                  log -> log.toString().contains("Memory Usage: \n") &&
                      log.toString().contains("Large buffers outstanding: ") &&
                      log.toString().contains("Normal buffers outstanding: ") &&
                      log.getLevel().equals(Level.TRACE)
              );
        }
        if (result) {
          break;
        }
      }
      assertTrue("Log messages are:\n" +
              memoryLogsAppender.list.stream().map(ILoggingEvent::toString).collect(Collectors.joining("\n")),
          result);
    } finally {
      memoryLogsAppender.stop();
      logger.detachAppender(memoryLogsAppender);
      logger.setLevel(null);
    }
  }
}
