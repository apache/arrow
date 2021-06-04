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

import static org.apache.arrow.flight.auth2.CallHeaderAuthenticator.AuthResult;

import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallInfo;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightServerMiddleware;
import org.apache.arrow.flight.RequestContext;

/**
 * Middleware that's used to validate credentials during the handshake and verify
 * the bearer token in subsequent requests.
 */
public class ServerCallHeaderAuthMiddleware implements FlightServerMiddleware {
  /**
   * Factory for accessing ServerAuthMiddleware.
   */
  public static class Factory implements FlightServerMiddleware.Factory<ServerCallHeaderAuthMiddleware> {
    private final CallHeaderAuthenticator authHandler;

    /**
     * Construct a factory with the given auth handler.
     * @param authHandler The auth handler what will be used for authenticating requests.
     */
    public Factory(CallHeaderAuthenticator authHandler) {
      this.authHandler = authHandler;
    }

    @Override
    public ServerCallHeaderAuthMiddleware onCallStarted(CallInfo callInfo, CallHeaders incomingHeaders,
                                                        RequestContext context) {
      final AuthResult result = authHandler.authenticate(incomingHeaders);
      context.put(Auth2Constants.PEER_IDENTITY_KEY, result.getPeerIdentity());
      return new ServerCallHeaderAuthMiddleware(result);
    }
  }

  private final AuthResult authResult;

  public ServerCallHeaderAuthMiddleware(AuthResult authResult) {
    this.authResult = authResult;
  }

  @Override
  public void onBeforeSendingHeaders(CallHeaders outgoingHeaders) {
    authResult.appendToOutgoingHeaders(outgoingHeaders);
  }

  @Override
  public void onCallCompleted(CallStatus status) {
  }

  @Override
  public void onCallErrored(Throwable err) {
  }
}
