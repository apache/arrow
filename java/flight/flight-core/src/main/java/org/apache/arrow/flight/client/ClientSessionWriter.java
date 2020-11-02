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

package org.apache.arrow.flight.client;

import java.util.function.Consumer;

import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightConstants;
import org.apache.arrow.util.VisibleForTesting;

/**
 * Session ID from the server.
 */
public class ClientSessionWriter implements Consumer<CallHeaders> {
  private volatile String sessionId;

  /**
   * Retrieves the current stored session ID.
   *
   * @return the current session ID.
   */
  @VisibleForTesting
  public String getSessionId() {
    return this.sessionId;
  }

  /**
   * Sets the session ID.
   *
   * The session ID is not available when the ClientSessionWriter is instantiated.
   * The setting of session ID is deferred until one is provided by the server.
   *
   * @param sessionId the session ID from the server.
   * @throws org.apache.arrow.flight.FlightRuntimeException if session ID provided does not match the
   *         the existing session ID stored in this ClientSessionWriter.
   */
  public void setSessionId(String sessionId) {
    synchronized (this) {
      if (this.sessionId != null) {
        if (!sessionId.equals(this.sessionId)) {
          throw CallStatus.UNAUTHENTICATED.toRuntimeException();
        }
      } else {
        this.sessionId = sessionId;
      }

    }
  }

  @Override
  public void accept(CallHeaders outgoingHeaders) {
    if (sessionId != null) {
      outgoingHeaders.insert(FlightConstants.SESSION_HEADER, sessionId);
    }
  }
}
