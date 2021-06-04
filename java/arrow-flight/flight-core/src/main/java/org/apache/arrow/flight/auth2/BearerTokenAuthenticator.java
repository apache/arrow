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

/**
 * Partial implementation of {@link CallHeaderAuthenticator} for bearer-token based authentication.
 */
public abstract class BearerTokenAuthenticator implements CallHeaderAuthenticator {

  final CallHeaderAuthenticator initialAuthenticator;

  public BearerTokenAuthenticator(CallHeaderAuthenticator initialAuthenticator) {
    this.initialAuthenticator = initialAuthenticator;
  }

  @Override
  public AuthResult authenticate(CallHeaders incomingHeaders) {
    // Check if headers contain a bearer token and if so, validate the token.
    final String bearerToken =
        AuthUtilities.getValueFromAuthHeader(incomingHeaders, Auth2Constants.BEARER_PREFIX);
    if (bearerToken != null) {
      return validateBearer(bearerToken);
    }

    // Delegate to the basic auth handler to do the validation.
    final CallHeaderAuthenticator.AuthResult result = initialAuthenticator.authenticate(incomingHeaders);
    return getAuthResultWithBearerToken(result);
  }

  /**
   * Callback to run when the initial authenticator succeeds.
   * @param authResult A successful initial authentication result.
   * @return an alternate AuthResult based on the original AuthResult that will write a bearer token to output headers.
   */
  protected abstract AuthResult getAuthResultWithBearerToken(AuthResult authResult);

  /**
   * Validate the bearer token.
   * @param bearerToken The bearer token to validate.
   * @return A successful AuthResult if validation succeeded.
   * @throws Exception If the token validation fails.
   */
  protected abstract AuthResult validateBearer(String bearerToken);

}
