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

import java.util.concurrent.atomic.AtomicReference;

import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallInfo;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightClientMiddleware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Middleware for capturing bearer tokens sent back from the Flight server.
 */
public class ClientBearerTokenMiddleware implements FlightClientMiddleware {
  private static final Logger logger = LoggerFactory.getLogger(ClientBearerTokenMiddleware.class);

  private final Factory factory;

  /**
   * Factory used within FlightClient.
   */
  public static class Factory implements FlightClientMiddleware.Factory {
    private final AtomicReference<String> bearerToken = new AtomicReference<>();

    @Override
    public FlightClientMiddleware onCallStarted(CallInfo info) {
      logger.debug("Call name: {}", info.method().name());
      return new ClientBearerTokenMiddleware(this);
    }

    void setBearerToken(String bearerToken) {
      this.bearerToken.set(bearerToken);
    }

    public String getBearerToken() {
      return bearerToken.get();
    }
  }

  private ClientBearerTokenMiddleware(Factory factory) {
    this.factory = factory;
  }

  @Override
  public void onBeforeSendingHeaders(CallHeaders outgoingHeaders) {
  }

  @Override
  public void onHeadersReceived(CallHeaders incomingHeaders) {
    final String bearerValue = AuthUtilities.getValueFromAuthHeader(incomingHeaders, AuthConstants.BEARER_PREFIX);
    if (bearerValue != null) {
      factory.setBearerToken(bearerValue);
    }
  }

  @Override
  public void onCallCompleted(CallStatus status) {
    logger.debug("Request completed with status {}.", status);
  }
}
