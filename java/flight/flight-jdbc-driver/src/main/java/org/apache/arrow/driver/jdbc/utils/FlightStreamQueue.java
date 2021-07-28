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

package org.apache.arrow.driver.jdbc.utils;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.arrow.flight.FlightStream;

/**
 * Auxiliary class used to handle consuming of multiple {@link FlightStream}.
 * <p>
 * The usage follows this routine:
 * <ol>
 *   <li>Create a <code>FlightStreamQueue</code>;</li>
 *   <li>Call <code>addToQueue(FlightStream)</code> for all streams to be consumed;</li>
 *   <li>Call <code>next()</code> to get a FlightStream that is ready to consume</li>
 *   <li>Consume the given FlightStream and add it back to the queue - call <code>addToQueue(FlightStream)</code></li>
 *   <li>Repeat from (3) until <code>next()</code> returns null.</li>
 * </ol>
 */
public class FlightStreamQueue implements AutoCloseable {
  private static final int THREAD_POOL_SIZE = 4;

  private final ExecutorService executorService;
  private final BlockingQueue<FlightStream> flightStreamQueue;
  private final AtomicInteger pendingFutures;

  /**
   * Instantiate a new FlightStreamQueue.
   */
  public FlightStreamQueue() {
    executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
    flightStreamQueue = new LinkedBlockingQueue<>();
    pendingFutures = new AtomicInteger(0);
  }

  /**
   * Blocking request to get the next ready FlightStream in queue.
   *
   * @return a FlightStream that is ready to consume or null if all FlightStreams are ended.
   */
  public FlightStream next() {
    try {
      while (pendingFutures.getAndDecrement() > 0) {
        final FlightStream flightStream = flightStreamQueue.take();
        if (flightStream.getRoot().getRowCount() > 0) {
          return flightStream;
        }
      }
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

    return null;
  }

  /**
   * Adds given FlightStream to the queue.
   */
  public void addToQueue(FlightStream flightStream) {
    pendingFutures.incrementAndGet();
    executorService.submit(() -> {
      // FlightStream#next will block until new data can be read or stream is over.
      flightStream.next();
      flightStreamQueue.add(flightStream);
    });
  }

  @Override
  public void close() throws Exception {
    this.executorService.shutdown();
  }
}
