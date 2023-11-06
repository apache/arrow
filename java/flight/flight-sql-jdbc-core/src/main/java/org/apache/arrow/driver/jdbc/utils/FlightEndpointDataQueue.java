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

import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.arrow.driver.jdbc.client.CloseableEndpointStreamPair;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStream;
import org.apache.calcite.avatica.AvaticaConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class FlightEndpointDataQueue implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(FlightEndpointDataQueue.class);
  private final CompletionService<CloseableEndpointStreamPair> completionService;
  private final Set<Future<CloseableEndpointStreamPair>> futures = synchronizedSet(new HashSet<>());
  private final Set<CloseableEndpointStreamPair> endpointsToClose = synchronizedSet(new HashSet<>());
  private final AtomicBoolean closed = new AtomicBoolean();

  /**
   * Instantiate a new FlightStreamQueue.
   */
  protected FlightEndpointDataQueue(final CompletionService<CloseableEndpointStreamPair> executorService) {
    completionService = checkNotNull(executorService);
  }

  /**
   * Creates a new {@link FlightEndpointDataQueue} from the provided {@link ExecutorService}.
   *
   * @param service the service from which to create a new queue.
   * @return a new queue.
   */
  public static FlightEndpointDataQueue createNewQueue(final ExecutorService service) {
    return new FlightEndpointDataQueue(new ExecutorCompletionService<>(service));
  }

  /**
   * Gets whether this queue is closed.
   *
   * @return a boolean indicating whether this resource is closed.
   */
  public boolean isClosed() {
    return closed.get();
  }

  /**
   * Auxiliary functional interface for getting ready-to-consume FlightStreams.
   */
  @FunctionalInterface
  interface EndpointStreamSupplier {
    Future<CloseableEndpointStreamPair> get() throws SQLException;
  }

  private CloseableEndpointStreamPair next(final EndpointStreamSupplier endpointStreamSupplier) throws SQLException {
    checkOpen();
    while (!futures.isEmpty()) {
      final Future<CloseableEndpointStreamPair> future = endpointStreamSupplier.get();
      futures.remove(future);
      try {
        final CloseableEndpointStreamPair endpoint = future.get();
        // Get the next FlightStream that has a root with content.
        if (endpoint != null) {
          return endpoint;
        }
      } catch (final ExecutionException | InterruptedException | CancellationException e) {
        throw AvaticaConnection.HELPER.wrap(e.getMessage(), e);
      }
    }
    return null;
  }

  /**
   * Blocking request with timeout to get the next ready FlightStream in queue.
   *
   * @param timeoutValue the amount of time to be waited
   * @param timeoutUnit  the timeoutValue time unit
   * @return a FlightStream that is ready to consume or null if all FlightStreams are ended.
   */
  public CloseableEndpointStreamPair next(final long timeoutValue, final TimeUnit timeoutUnit)
      throws SQLException {
    return next(() -> {
      try {
        final Future<CloseableEndpointStreamPair> future = completionService.poll(timeoutValue, timeoutUnit);
        if (future != null) {
          return future;
        }
      } catch (final InterruptedException e) {
        throw new SQLTimeoutException("Query was interrupted", e);
      }

      throw new SQLTimeoutException(
          String.format("Query timed out after %d %s", timeoutValue, timeoutUnit));
    });
  }

  /**
   * Blocking request to get the next ready FlightStream in queue.
   *
   * @return a FlightStream that is ready to consume or null if all FlightStreams are ended.
   */
  public CloseableEndpointStreamPair next() throws SQLException {
    return next(() -> {
      try {
        return completionService.take();
      } catch (final InterruptedException e) {
        throw AvaticaConnection.HELPER.wrap(e.getMessage(), e);
      }
    });
  }

  /**
   * Checks if this queue is open.
   */
  public synchronized void checkOpen() {
    checkState(!isClosed(), format("%s closed", this.getClass().getSimpleName()));
  }

  /**
   * Readily adds given {@link FlightStream}s to the queue.
   */
  public void enqueue(final Collection<CloseableEndpointStreamPair> endpointRequests) {
    endpointRequests.forEach(this::enqueue);
  }

  /**
   * Adds given {@link FlightStream} to the queue.
   */
  public synchronized void enqueue(final CloseableEndpointStreamPair endpointRequest) {
    checkNotNull(endpointRequest);
    checkOpen();
    endpointsToClose.add(endpointRequest);
    futures.add(completionService.submit(() -> {
      // `FlightStream#next` will block until new data can be read or stream is over.
      while (endpointRequest.getStream().next()) {
        if (endpointRequest.getStream().getRoot().getRowCount() > 0) {
          return endpointRequest;
        }
      }
      return null;
    }));
  }

  private static boolean isCallStatusCancelled(final Exception e) {
    return e.getCause() instanceof FlightRuntimeException &&
        ((FlightRuntimeException) e.getCause()).status().code() == CallStatus.CANCELLED.code();
  }

  @Override
  public synchronized void close() throws SQLException {
    if (isClosed()) {
      return;
    }

    final Set<SQLException> exceptions = new HashSet<>();
    try {
      for (final CloseableEndpointStreamPair endpointToClose : endpointsToClose) {
        try {
          endpointToClose.getStream().cancel("Cancelling this FlightStream.", null);
        } catch (final Exception e) {
          final String errorMsg = "Failed to cancel a FlightStream.";
          LOGGER.error(errorMsg, e);
          exceptions.add(new SQLException(errorMsg, e));
        }
      }
      futures.forEach(future -> {
        try {
          // TODO: Consider adding a hardcoded timeout?
          future.get();
        } catch (final InterruptedException | ExecutionException e) {
          // Ignore if future is already cancelled
          if (!isCallStatusCancelled(e)) {
            final String errorMsg = "Failed consuming a future during close.";
            LOGGER.error(errorMsg, e);
            exceptions.add(new SQLException(errorMsg, e));
          }
        }
      });
      for (final CloseableEndpointStreamPair endpointToClose : endpointsToClose) {
        try {
          endpointToClose.close();
        } catch (final Exception e) {
          final String errorMsg = "Failed to close a FlightStream.";
          LOGGER.error(errorMsg, e);
          exceptions.add(new SQLException(errorMsg, e));
        }
      }
    } finally {
      endpointsToClose.clear();
      futures.clear();
      closed.set(true);
    }
    if (!exceptions.isEmpty()) {
      final SQLException sqlException = new SQLException("Failed to close streams.");
      exceptions.forEach(sqlException::setNextException);
      throw sqlException;
    }
  }
}
