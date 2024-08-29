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
 * This must only be used in FlightProducer implementations that are non-blocking.
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
    TIMEOUT,

    /**
     * Indicates that the wait was interrupted for a reason
     * unrelated to the listener itself.
     */
    OTHER
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
      listener.setOnReadyHandler(this::onReady);
      listener.setOnCancelHandler(this::onCancel);
    }

    @Override
    public WaitResult waitForListener(long timeout) {
      Preconditions.checkNotNull(listener);
      long remainingTimeout = timeout;
      final long startTime = System.currentTimeMillis();
      synchronized (lock) {
        while (!listener.isReady() && !listener.isCancelled()) {
          try {
            lock.wait(remainingTimeout);
            if (timeout != 0) { // If timeout was zero explicitly, we should never report timeout.
              remainingTimeout = startTime + timeout - System.currentTimeMillis();
              if (remainingTimeout <= 0) {
                return WaitResult.TIMEOUT;
              }
            }
            if (!shouldContinueWaiting(listener, remainingTimeout)) {
              return WaitResult.OTHER;
            }
          } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            return WaitResult.OTHER;
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

    /**
     * Interrupt waiting on the listener to change state.
     *
     * This method can be used in conjunction with
     * {@link #shouldContinueWaiting(FlightProducer.ServerStreamListener, long)} to allow FlightProducers to
     * terminate streams internally and notify clients.
     */
    public void interruptWait() {
      synchronized (lock) {
        lock.notifyAll();
      }
    }

    /**
     * Callback function to run to check if the listener should continue
     * to be waited on if it leaves the waiting state without being cancelled,
     * ready, or timed out.
     *
     * This method should be used to determine if the wait on the listener was interrupted explicitly using a
     * call to {@link #interruptWait()} or if it was woken up due to a spurious wake.
     */
    protected boolean shouldContinueWaiting(FlightProducer.ServerStreamListener listener, long remainingTimeout) {
      return true;
    }

    /**
     * Callback to execute when the listener becomes ready.
     */
    protected void readyCallback() {
    }

    /**
     * Callback to execute when the listener is cancelled.
     */
    protected void cancelCallback() {
    }

    private void onReady() {
      synchronized (lock) {
        readyCallback();
        lock.notifyAll();
      }
    }

    private void onCancel() {
      synchronized (lock) {
        cancelCallback();
        lock.notifyAll();
      }
    }
  }
}
