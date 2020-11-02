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

package org.apache.arrow.flight.auth2;

import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallInfo;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightClientMiddleware;
import org.apache.arrow.flight.grpc.CredentialCallOption;

/**
 * Middleware for capturing bearer tokens sent back from the Flight server.
 */
public class ClientIncomingAuthHeaderMiddleware implements FlightClientMiddleware {
  private final Factory factory;

  /**
   * Factory used within FlightClient.
   */
  public static class Factory implements FlightClientMiddleware.Factory {
    private final ClientHeaderHandler headerHandler;
    private CredentialCallOption credentialCallOption;

    /**
     * Construct a factory with the given header handler.
     * @param headerHandler The header handler that will be used for handling incoming headers from the flight server.
     */
    public Factory(ClientHeaderHandler headerHandler) {
      this.headerHandler = headerHandler;
    }

    @Override
    public FlightClientMiddleware onCallStarted(CallInfo info) {
      return new ClientIncomingAuthHeaderMiddleware(this);
    }

    void setCredentialCallOption(CredentialCallOption callOption) {
      this.credentialCallOption = callOption;
    }

    public CredentialCallOption getCredentialCallOption() {
      return credentialCallOption;
    }
  }

  private ClientIncomingAuthHeaderMiddleware(Factory factory) {
    this.factory = factory;
  }

  @Override
  public void onBeforeSendingHeaders(CallHeaders outgoingHeaders) {
  }

  @Override
  public void onHeadersReceived(CallHeaders incomingHeaders) {
    factory.setCredentialCallOption(
            factory.headerHandler.getCredentialCallOptionFromIncomingHeaders(incomingHeaders));
  }

  @Override
  public void onCallCompleted(CallStatus status) {
  }
}
