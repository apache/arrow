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

import java.util.Objects;

/**
 * Server-side middleware for Flight calls.
 *
 * <p>Middleware are instantiated per-call.
 *
 * <p>Methods are not guaranteed to be called on any particular thread, relative to the thread that Flight requests are
 * executed on. Do not depend on thread-local storage; instead, use state on the middleware instance. Service
 * implementations may communicate with middleware implementations through
 * {@link org.apache.arrow.flight.FlightProducer.CallContext#getMiddleware(Key)}. Methods on the middleware instance
 * are non-reentrant, that is, a particular RPC will not make multiple concurrent calls to methods on a single
 * middleware instance. However, methods on the factory instance are expected to be thread-safe, and if the factory
 * instance returns the same middleware object more than once, then that middleware object must be thread-safe.
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
     * @param context Context about the current request.
     *
     * @throws FlightRuntimeException if the middleware wants to reject the call with the given status
     */
    T onCallStarted(CallInfo info, CallHeaders incomingHeaders, RequestContext context);
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
      this.key = Objects.requireNonNull(key, "Key must not be null.");
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
  void onBeforeSendingHeaders(CallHeaders outgoingHeaders);

  /**
   * Callback for when the underlying transport has completed a call.
   * @param status Whether the call completed successfully or not.
   */
  void onCallCompleted(CallStatus status);

  /**
   * Callback for when an RPC method implementation throws an uncaught exception.
   *
   * <p>May be called multiple times, and may be called before or after {@link #onCallCompleted(CallStatus)}.
   * Generally, an uncaught exception will end the call with a error {@link CallStatus}, and will be reported to {@link
   * #onCallCompleted(CallStatus)}, but not necessarily this method.
   *
   * @param err The exception that was thrown.
   */
  void onCallErrored(Throwable err);
}
