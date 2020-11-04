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
 * ServerSessionHandler interface to retrieve the current session.
 */
public interface ServerSessionHandler {
  /**
   * A session handler that does not support sessions.
   * It is used as the default handler for a Flight server with no session capabilities.
   */
  ServerSessionHandler NO_OP = new ServerSessionHandler() {
    @Override
    public String beginSession(CallHeaders headers) {
      return null;
    }

    @Override
    public String getSession(CallHeaders headers) {
      return null;
    }
  };

  /**
   * Creates and return the session ID.
   *
   * @return the session ID for the current session.
   */
  String beginSession(CallHeaders headers);

  /**
   * Retrieves the current session for the corresponding client.
   *
   * @param headers CallHeaders to inspect.
   * @return the corresponding session.
   * @throws FlightRuntimeException
   *     with CallStatus {@code UNAUTHENTICATED} if the session has expired.
   *     with CallStatus {@code INVALID_ARGUMENT} if the session header contains unrecognised properties.
   *     with CallStatus {@code NOT_FOUND} if the session header is not found.
   */
  String getSession(CallHeaders headers);
}
