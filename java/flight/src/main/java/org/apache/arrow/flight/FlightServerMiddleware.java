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

/**
 * Server-side middleware for Flight calls.
 *
 * <p>Middleware are instantiated per-call.
 *
 * <p>Methods are not guaranteed to be called on any particular thread, relative to the thread that Flight requests are
 * executed on. Do not depend on thread-local storage; instead, use state on the middleware instance. Service
 * implementations may communicate with middleware implementations through
 * {@link org.apache.arrow.flight.FlightProducer.CallContext#getMiddleware(Key)}.
 */
public interface FlightServerMiddleware {

  /**
   * A factory for Flight server middleware.
   * @param <T> The middleware type.
   */
  interface Factory<T extends FlightServerMiddleware> {
    /**
     * A callback for when the call starts.
     *
     * @param info Details about the call.
     * @param incomingHeaders A mutable set of request headers.
     *
     * @throws FlightRuntimeException if the middleware wants to reject the call with the given status
     */
    T startCall(CallInfo info, CallHeaders incomingHeaders);
  }

  /**
   * A key for Flight server middleware. On a server, middleware instances are identified by this key.
   *
   * <p>Keys use reference equality, so instances should be shared.
   *
   * @param <T> The middleware class stored in this key. This provides a compile-time check when retrieving instances.
   */
  class Key<T extends FlightServerMiddleware> {
    final String key;

    Key(String key) {
      this.key = key;
    }

    /**
     * Create a new key for the given type.
     */
    public static <T extends FlightServerMiddleware> Key<T> of(String key) {
      return new Key<>(key);
    }
  }

  /**
   * Callback for when the underlying transport is about to send response headers.
   *
   * @param outgoingHeaders A mutable set of response headers. These can be manipulated to send different headers to the
   *     client.
   */
  void sendingHeaders(CallHeaders outgoingHeaders);

  /**
   * Callback for when the underlying transport has completed a call.
   * @param status Whether the call completed successfully or not.
   */
  void callCompleted(CallStatus status);

  /**
   * Callback for when an RPC method implementation throws an uncaught exception.
   *
   * <p>May be called multiple times, and may be called before or after {@link #callCompleted(CallStatus)}.
   * Generally, an uncaught exception will end the call with a error {@link CallStatus}, and will be reported to {@link
   * #callCompleted(CallStatus)}, but not necessarily this method.
   *
   * @param err The exception that was thrown.
   */
  void callErrored(Throwable err);
}
