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
 * Client-side middleware for Flight.
 *
 * <p>Middleware are instantiated per-call and should store state in the middleware instance.
 */
public interface FlightClientMiddleware {
  /**
   * A callback used before request headers are sent. The headers may be manipulated.
   */
  void onBeforeSendingHeaders(CallHeaders outgoingHeaders);

  /**
   * A callback called after response headers are received. The headers may be manipulated.
   */
  void onHeadersReceived(CallHeaders incomingHeaders);

  /**
   * A callback called after the call completes.
   */
  void onCallCompleted(CallStatus status);

  /**
   * A factory for client middleware instances.
   */
  interface Factory {
    /**
     * Create a new middleware instance for the given call.
     *
     * @throws FlightRuntimeException if the middleware wants to reject the call with the given status
     */
    FlightClientMiddleware onCallStarted(CallInfo info);
  }
}
