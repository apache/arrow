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

import java.util.Collection;
import java.util.HashSet;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.arrow.flight.FlightStream;

/**
 * Auxiliary class used to handle consuming of multiple {@link FlightStream}.
 * <p>
 * The usage follows this routine:
 * <ol>
 *   <li>Create a <code>FlightStreamQueue</code>;</li>
 *   <li>Call <code>enqueue(FlightStream)</code> for all streams to be consumed;</li>
 *   <li>Call <code>next()</code> to get a FlightStream that is ready to consume</li>
 *   <li>Consume the given FlightStream and add it back to the queue - call <code>enqueue(FlightStream)</code></li>
 *   <li>Repeat from (3) until <code>next()</code> returns null.</li>
 * </ol>
 */
public class FlightStreamQueue implements AutoCloseable {
  private static final int THREAD_POOL_SIZE = 4;

  private final ExecutorService executorService;
  private final CompletionService<FlightStream> completionService;
  private final Collection<Future<?>> futures;

  /**
   * Instantiate a new FlightStreamQueue.
   */
  public FlightStreamQueue() {
    executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
    completionService = new ExecutorCompletionService<>(executorService);
    futures = new HashSet<>();
  }

  /**
   * Blocking request to get the next ready FlightStream in queue.
   *
   * @return a FlightStream that is ready to consume or null if all FlightStreams are ended.
   */
  public FlightStream next() {
    while (!futures.isEmpty()) {
      try {
        final Future<FlightStream> future = completionService.take();
        futures.remove(future);

        final FlightStream flightStream = future.get();
        if (flightStream.getRoot().getRowCount() > 0) {
          return flightStream;
        }
      } catch (ExecutionException e) {
        // Try next stream
      } catch (InterruptedException | CancellationException e) {
        return null;
      }
    }

    return null;
  }

  /**
   * Adds given FlightStream to the queue.
   */
  public void enqueue(FlightStream flightStream) {
    final Future<FlightStream> future = completionService.submit(() -> {
      // FlightStream#next will block until new data can be read or stream is over.
      flightStream.next();

      return flightStream;
    });
    futures.add(future);
  }

  @Override
  public void close() throws Exception {
    futures.forEach(future -> future.cancel(true));
    this.executorService.shutdown();
  }
}
