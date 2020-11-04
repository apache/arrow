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

import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallInfo;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightClientMiddleware;
import org.apache.arrow.flight.FlightConstants;

/**
 * Client middleware to re-use the existing session provided by the server.
 */
public class ClientSessionMiddleware implements FlightClientMiddleware {
  /**
   * Factory to create instances of ClientSessionMiddleware for each call.
   */
  public static class Factory implements FlightClientMiddleware.Factory {
    private final ClientSessionWriter sessionWriter;

    public Factory() {
      sessionWriter = new ClientSessionWriter();
    }

    @Override
    public FlightClientMiddleware onCallStarted(CallInfo info) {
      return new ClientSessionMiddleware(sessionWriter);
    }
  }

  private final ClientSessionWriter sessionWriter;

  public ClientSessionMiddleware(ClientSessionWriter sessionWriter) {
    this.sessionWriter = sessionWriter;
  }

  @Override
  public void onBeforeSendingHeaders(CallHeaders outgoingHeaders) {
    sessionWriter.accept(outgoingHeaders);
  }

  @Override
  public void onHeadersReceived(CallHeaders incomingHeaders) {
    final String sessionId = incomingHeaders.get(FlightConstants.SET_SESSION_HEADER);
    sessionWriter.setSession(sessionId);
  }

  @Override
  public void onCallCompleted(CallStatus status) {
    // No-op
  }
}
