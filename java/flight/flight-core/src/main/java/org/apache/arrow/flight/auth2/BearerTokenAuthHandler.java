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
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightRuntimeException;

/**
 * Partial implementation of ServerAuthHandler for bearer-token based authentication.
 */
abstract class BearerTokenAuthHandler implements CallHeaderAuthenticator {

  final BasicCallHeaderAuthenticator basicAuthHandler;

  public BearerTokenAuthHandler(BasicCallHeaderAuthenticator basicAuthHandler) {
    this.basicAuthHandler = basicAuthHandler;
  }

  @Override
  public AuthResult authenticate(CallHeaders incomingHeaders) {

    // Check if header contain a bearer token and validate the token.
    final String bearerToken =
            AuthUtilities.getValueFromAuthHeader(incomingHeaders, Auth2Constants.BEARER_PREFIX);
    if (bearerToken != null) {
      final String peerIdentity;
      try {
        peerIdentity = validateBearer(bearerToken);
      } catch (FlightRuntimeException ex) {
        throw ex;
      } catch (Exception ex) {
        throw CallStatus.UNAUTHENTICATED.withCause(ex).toRuntimeException();
      }

      return new AuthResult() {
        @Override
        public String getPeerIdentity() {
          return peerIdentity;
        }

        @Override
        public HeaderMetadata getHeaderMetadata() {
          return new HeaderMetadata() {
            @Override
            public String getKey() {
              return Auth2Constants.AUTHORIZATION_HEADER;
            }

            @Override
            public String getValuePrefix() {
              return Auth2Constants.BEARER_PREFIX;
            }

            @Override
            public String getValue() {
              return bearerToken;
            }
          };
        }

        @Override
        public void appendToOutgoingHeaders(CallHeaders outgoingHeaders) {
          HeaderMetadata metadata = this.getHeaderMetadata();
          if (null == AuthUtilities.getValueFromAuthHeader(outgoingHeaders, metadata.getValuePrefix())) {
            outgoingHeaders.insert(metadata.getKey(), metadata.getValuePrefix() + metadata.getValue());
          }
        }
      };
    }

    // Delegate to the basic auth handler to do the validation.
    final CallHeaderAuthenticator.AuthResult result = basicAuthHandler.authenticate(incomingHeaders);
    registerBearer(result);
    return result;
  }

  /**
   * Extracts the bearer token from the authResult and stores the bearer token.
   * @param authResult The auth result to extract the bearer token from.
   */
  protected abstract void registerBearer(AuthResult authResult);

  /**
   * Validate the bearer token.
   * @param bearerToken The bearer token to validate.
   * @return The peerIdentity if the token is valid.
   * @throws Exception If the token validation fails.
   */
  protected abstract String validateBearer(String bearerToken) throws Exception;

}
