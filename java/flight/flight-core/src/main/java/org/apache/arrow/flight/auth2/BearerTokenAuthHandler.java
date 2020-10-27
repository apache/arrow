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

import java.util.Optional;

import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallStatus;

/**
 * Partial implementation of ServerAuthHandler for bearer-token based authentication.
 */
abstract class BearerTokenAuthHandler implements CallHeaderAuthenticator {
  @Override
  public AuthResult authenticate(CallHeaders headers) {
    final String bearerToken = AuthUtilities.getValueFromAuthHeader(headers, Auth2Constants.BEARER_PREFIX);
    if (bearerToken == null) {
      throw CallStatus.UNAUTHENTICATED.toRuntimeException();
    }

    if (!validateBearer(bearerToken)) {
      throw CallStatus.UNAUTHORIZED.toRuntimeException();
    }

    return new AuthResult() {
      @Override
      public String getPeerIdentity() {
        return getIdentityForBearerToken(bearerToken);
      }

      @Override
      public Optional<String> getBearerToken() {
        return Optional.of(bearerToken);
      }

      @Override
      public void appendToOutgoingHeaders(CallHeaders outgoingHeaders) {
        if (null == AuthUtilities.getValueFromAuthHeader(outgoingHeaders, Auth2Constants.BEARER_PREFIX)) {
          outgoingHeaders.insert(Auth2Constants.AUTHORIZATION_HEADER, Auth2Constants.BEARER_PREFIX + bearerToken);
        }
      }
    };
  }

  protected abstract String getIdentityForBearerToken(String bearerToken);

}
