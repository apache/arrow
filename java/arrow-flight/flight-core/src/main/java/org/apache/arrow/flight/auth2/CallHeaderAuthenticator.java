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
import org.apache.arrow.flight.FlightRuntimeException;

/**
 * Interface for Server side authentication handlers.
 *
 * A CallHeaderAuthenticator is used by {@link ServerCallHeaderAuthMiddleware} to validate headers sent by a Flight
 * client for authentication purposes. The headers validated do not necessarily have to be Authorization headers.
 *
 * The workflow is that the FlightServer will intercept headers on a request, validate the headers, and
 * either send back an UNAUTHENTICATED error, or succeed and potentially send back additional headers to the client.
 *
 * Implementations of CallHeaderAuthenticator should take care not to provide leak confidential details (such as
 * indicating if usernames are valid or not) for security reasons when reporting errors back to clients.
 *
 * Example CallHeaderAuthenticators provided include:
 * The {@link BasicCallHeaderAuthenticator} will authenticate basic HTTP credentials.
 *
 * The {@link BearerTokenAuthenticator} will authenticate basic HTTP credentials initially, then also send back a
 * bearer token that the client can use for subsequent requests. The {@link GeneratedBearerTokenAuthenticator} will
 * provide internally generated bearer tokens and maintain a cache of them.
 */
public interface CallHeaderAuthenticator {

  /**
   * Encapsulates the result of the {@link CallHeaderAuthenticator} analysis of headers.
   *
   * This includes the identity of the incoming user and any outbound headers to send as a response to the client.
   */
  interface AuthResult {
    /**
     * The peer identity that was determined by the handshake process based on the
     * authentication credentials supplied by the client.
     *
     * @return The peer identity.
     */
    String getPeerIdentity();

    /**
     * Appends a header to the outgoing call headers.
     * @param outgoingHeaders The outgoing headers.
     */
    default void appendToOutgoingHeaders(CallHeaders outgoingHeaders) {

    }
  }

  /**
   * Validate the auth headers sent by the client.
   *
   * @param incomingHeaders The incoming headers to authenticate.
   * @return an auth result containing a peer identity and optionally a bearer token.
   * @throws FlightRuntimeException with CallStatus.UNAUTHENTICATED if credentials were not supplied
   *     or if credentials were supplied but were not valid.
   */
  AuthResult authenticate(CallHeaders incomingHeaders);

  /**
   * An auth handler that does nothing.
   */
  CallHeaderAuthenticator NO_OP = new CallHeaderAuthenticator() {
    @Override
    public AuthResult authenticate(CallHeaders incomingHeaders) {
      return () -> "";
    }
  };
}
