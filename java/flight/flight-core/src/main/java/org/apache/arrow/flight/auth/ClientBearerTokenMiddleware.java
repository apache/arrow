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

package org.apache.arrow.flight.auth;

import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallInfo;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightClientMiddleware;
import org.apache.arrow.flight.FlightRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Middleware for capturing and sending back bearer tokens.
 */
public class ClientBearerTokenMiddleware implements FlightClientMiddleware {
  private static final Logger logger = LoggerFactory.getLogger(ClientBearerTokenMiddleware.class);

  private final String bearerToken;

  /**
   * Factory used within FlightClient.
   */
  public static class Factory implements FlightClientMiddleware.Factory {
    private final String bearerToken = null;

    @Override
    public FlightClientMiddleware onCallStarted(CallInfo info) {
      if (info.method().equals(AuthConstants.HANDSHAKE_DESCRIPTOR_NAME)) {
        return new ClientAuthHandshakeMiddleware(this);
      }

      if (bearerToken == null) {
        logger.error("Tried to execute a method without getting a bearer token from the authorization process.");
        throw new FlightRuntimeException(CallStatus.INTERNAL);
      }
      return new ClientBearerTokenMiddleware(bearerToken);
    }

    void setBearerToken(String bearerToken) {
      if (bearerToken != null) {
        logger.error("Executed the authentication process twice.");
        throw new FlightRuntimeException(CallStatus.INTERNAL);
      }
    }
  }

  private ClientBearerTokenMiddleware(String bearerToken) {
    this.bearerToken = bearerToken;
  }

  @Override
  public void onBeforeSendingHeaders(CallHeaders outgoingHeaders) {
    outgoingHeaders.insert(AuthConstants.AUTHORIZATION_HEADER, AuthConstants.BEARER_PREFIX + bearerToken);
  }

  @Override
  public void onHeadersReceived(CallHeaders incomingHeaders) {

  }

  @Override
  public void onCallCompleted(CallStatus status) {

  }

  /**
   * Middleware for capturing the bearer token to use in subsequent requests.
   */
  static class ClientAuthHandshakeMiddleware implements FlightClientMiddleware {
    private final ClientBearerTokenMiddleware.Factory factory;

    private ClientAuthHandshakeMiddleware(ClientBearerTokenMiddleware.Factory factory) {
      this.factory = factory;
    }

    @Override
    public void onBeforeSendingHeaders(CallHeaders outgoingHeaders) {
      // Auth headers are specified by setting CallCredentials on the
      // FlightClient.Builder, rather than through middleware.
    }

    @Override
    public void onHeadersReceived(CallHeaders incomingHeaders) {
      final String bearerValue = AuthUtilities.getValueFromAuthHeader(incomingHeaders, AuthConstants.BEARER_PREFIX);
      if (bearerValue == null) {
        logger.error("Server did not send a bearer token after successful authentication.");
        throw new FlightRuntimeException(CallStatus.INTERNAL);
      }

      factory.setBearerToken(bearerValue);
    }

    @Override
    public void onCallCompleted(CallStatus status) {
      logger.info("Handshake completed successfully.");
    }
  }
}
