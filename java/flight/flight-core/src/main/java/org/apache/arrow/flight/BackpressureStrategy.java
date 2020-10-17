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

package org.apache.arrow.flight;

import org.apache.arrow.vector.VectorSchemaRoot;

import com.google.common.base.Preconditions;

/**
 * Helper interface to dynamically handle backpressure when implementing FlightProducers.
 */
public interface BackpressureStrategy {
  /**
   * The state of the client after a call to waitForListener.
   */
  enum WaitResult {
    /**
     * Listener is ready.
     */
    READY,

    /**
     * Listener was cancelled by the client.
     */
    CANCELLED,

    /**
     * Timed out waiting for the listener to change state.
     */
    TIMEOUT
  }

  /**
   * Set up operations to work against the given listener.
   *
   * This must be called exactly once and before any calls to {@link #waitForListener(long)} and
   * {@link OutboundStreamListener#start(VectorSchemaRoot)}
   * @param listener The listener this strategy applies to.
   */
  void register(FlightProducer.ServerStreamListener listener);

  /**
   * Waits for the listener to be ready or cancelled up to the given timeout.
   *
   * @param timeout The timeout in milliseconds. Infinite if timeout is <= 0.
   * @return The result of the wait.
   */
  WaitResult waitForListener(long timeout);

  /**
   * A back pressure strategy that uses callbacks to notify when the client is ready or cancelled.
   */
  class CallbackBackpressureStrategy implements BackpressureStrategy {
    private final Object lock = new Object();
    private FlightProducer.ServerStreamListener listener;

    @Override
    public void register(FlightProducer.ServerStreamListener listener) {
      this.listener = listener;
      listener.setOnReadyHandler(this::onReadyOrCancel);
      listener.setOnCancelHandler(this::onReadyOrCancel);
    }

    @Override
    public WaitResult waitForListener(long timeout) {
      Preconditions.checkNotNull(listener);
      final long startTime = System.currentTimeMillis();
      synchronized (lock) {
        while (!listener.isReady() && !listener.isCancelled()) {
          try {
            lock.wait(timeout);
            if (System.currentTimeMillis() > startTime + timeout) {
              return WaitResult.TIMEOUT;
            }
          } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            return WaitResult.CANCELLED;
          }
        }

        if (listener.isReady()) {
          return WaitResult.READY;
        } else if (listener.isCancelled()) {
          return WaitResult.CANCELLED;
        } else if (System.currentTimeMillis() > startTime + timeout) {
          return WaitResult.TIMEOUT;
        }
        throw new RuntimeException("Invalid state when waiting for listener.");
      }
    }

    void onReadyOrCancel() {
      synchronized (lock) {
        lock.notifyAll();
      }
    }
  }
}
