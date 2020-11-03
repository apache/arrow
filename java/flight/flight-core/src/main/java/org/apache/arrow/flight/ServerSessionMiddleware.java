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
 * Middleware to add a SET-SESSION header.
 */
public class ServerSessionMiddleware implements FlightServerMiddleware {

  /**
   * Factory class to provide instances of ServerSessionMiddleware for each call.
   */
  public static class Factory implements FlightServerMiddleware.Factory<ServerSessionMiddleware> {
    private final ServerSessionHandler sessionHandler;

    public Factory(ServerSessionHandler sessionHandler) {
      this.sessionHandler = sessionHandler;
    }

    @Override
    public ServerSessionMiddleware onCallStarted(CallInfo info, CallHeaders incomingHeaders,
                                                 RequestContext context) {
      if (incomingHeaders.containsKey(FlightConstants.SESSION_HEADER)) {
        // Client is re-using existing session ID.
        // ServerSessionHandler validates client session ID before proceeding.
        final String existingSessionId = incomingHeaders.get(FlightConstants.SESSION_HEADER);
        if (!sessionHandler.isValid(existingSessionId)) {
          throw CallStatus.UNAUTHENTICATED.toRuntimeException();
        }
      } else {
        // No existing session ID provided, establishing a new session.
        // Insert SET-SESSION header if ServerSessionHandler returns a non-null session ID.
        final String sessionId = sessionHandler.getSessionId();
        if (sessionId != null) {
          incomingHeaders.insert(FlightConstants.SESSION_HEADER, sessionId);
        }
      }

      return new ServerSessionMiddleware(sessionHandler);
    }
  }

  private ServerSessionHandler sessionHandler;

  private ServerSessionMiddleware(ServerSessionHandler sessionHandler) {
    this.sessionHandler = sessionHandler;
  }

  @Override
  public void onBeforeSendingHeaders(CallHeaders outgoingHeaders) {
    String sessionId = sessionHandler.getSessionId();
    if (sessionId != null) {
      outgoingHeaders.insert(FlightConstants.SESSION_HEADER, sessionId);
    }
  }

  @Override
  public void onCallCompleted(CallStatus status) {
    // No-op
  }

  @Override
  public void onCallErrored(Throwable err) {
    // No-op
  }
}
