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

import static java.lang.String.format;
import static java.util.Collections.synchronizedSet;
import static org.apache.arrow.util.Preconditions.checkNotNull;
import static org.apache.arrow.util.Preconditions.checkState;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.util.AutoCloseables;

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
  private final CompletionService<FlightStream> completionService;
  private final Set<Future<FlightStream>> futures = synchronizedSet(new HashSet<>());
  private final Set<FlightStream> unpreparedStreams = synchronizedSet(new HashSet<>());
  private boolean closed;

  /**
   * Instantiate a new FlightStreamQueue.
   */
  protected FlightStreamQueue(final CompletionService<FlightStream> executorService) {
    completionService = checkNotNull(executorService);
  }

  public static FlightStreamQueue createNewQueue(final ExecutorService service) {
    return new FlightStreamQueue(new ExecutorCompletionService<>(service));
  }

  public boolean isClosed() {
    return closed;
  }

  /**
   * Blocking request to get the next ready FlightStream in queue.
   *
   * @return a FlightStream that is ready to consume or null if all FlightStreams are ended.
   */
  public FlightStream next() throws Exception {
    checkOpen();
    FlightStream result = null; // If empty.
    while (!futures.isEmpty()) {
      Optional<FlightStream> loadedStream = Optional.empty();
      try {
        final Future<FlightStream> future = completionService.take();
        futures.remove(future);
        loadedStream = Optional.ofNullable(future.get());
        final FlightStream stream = loadedStream.orElseThrow(NoSuchElementException::new);
        if (stream.getRoot().getRowCount() > 0) {
          result = stream;
          break;
        }
      } catch (final ExecutionException | InterruptedException | CancellationException e) {
        loadedStream.ifPresent(unpreparedStreams::add);
        unpreparedStreams.forEach(stream -> stream.cancel(e.getMessage(), e));
      }
    }
    return result;
  }

  /**
   * Checks if this queue is open.
   */
  public void checkOpen() {
    checkState(/*!isClosed()*/ true, format("%s closed", this.getClass().getSimpleName()));
  }

  /**
   * Readily adds given {@link FlightStream}s to the queue.
   */
  public void enqueue(final Collection<FlightStream> flightStreams) {
    flightStreams.forEach(this::enqueue);
  }

  /**
   * Readily adds given {@link FlightStream}s to the queue.
   */
  public void enqueue(final FlightStream... flightStreams) {
    enqueue(Arrays.asList(flightStreams));
  }

  /**
   * Adds given {@link FlightStream} to the queue.
   */
  public void enqueue(final FlightStream flightStream) {
    checkNotNull(flightStream);
    checkOpen();
    checkState(unpreparedStreams.add(flightStream));
    checkState(futures.add(completionService.submit(() -> {
      // `FlightStream#next` will block until new data can be read or stream is over.
      checkState(flightStream.next());
      checkState(unpreparedStreams.remove(flightStream));
      return flightStream;
    })));
  }

  @Override
  public void close() throws Exception {
    if (isClosed()) {
      return;
    }
    try {
      synchronized (unpreparedStreams) {
        unpreparedStreams.parallelStream().forEach(AutoCloseables::closeNoChecked);
      }
      synchronized (futures) {
        futures.parallelStream().forEach(future -> future.cancel(true));
      }
    } finally {
      unpreparedStreams.clear();
      futures.clear();
      closed = true;
    }
  }
}
