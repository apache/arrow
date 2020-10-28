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
 */
public interface CallHeaderAuthenticator {
  /**
   * The result of the server analyzing authentication headers.
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
     *
     * @param outgoingHeaders The outgoing call headers to append the header to.
     */
    void appendToOutgoingHeaders(CallHeaders outgoingHeaders);
  }

  /**
   * Validate the auth headers sent by the client.
   *
   * @param headers The headers to authenticate.
   * @return a handshake result containing a peer identity and optionally a bearer token.
   * @throws FlightRuntimeException with CallStatus.UNAUTHENTICATED if credentials were not supplied
   *     or CallStatus.UNAUTHORIZED if credentials were supplied but were not valid.
   */
  AuthResult authenticate(CallHeaders headers);

  /**
   * An auth handler that does nothing.
   */
  CallHeaderAuthenticator NO_OP = new CallHeaderAuthenticator() {
    @Override
    public AuthResult authenticate(CallHeaders headers) {
      return new AuthResult() {
        @Override
        public String getPeerIdentity() {
          return "";
        }

        @Override
        public void appendToOutgoingHeaders(CallHeaders outgoingHeaders) {

        }
      };
    }
  };
}
