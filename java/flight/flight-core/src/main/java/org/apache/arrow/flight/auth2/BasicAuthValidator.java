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

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Optional;

import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.auth2.CallHeaderAuthenticator.AuthResult;

/**
 * An AuthValidator for Token generation and username/password validation.
 */
public class BasicAuthValidator implements BasicCallHeaderAuthenticator.AuthValidator {

  private final CredentialValidator credentialValidator;
  private final AuthTokenManager authTokenManager;

  /**
   * Creates a validator with supplied AuthTokenManager to generate token
   * and CredentialValidator to validate username/password. 
   * @param authTokenManager AuthTokenManager used to generate tokens.
   * @param credentialValidator CredentialValidator used to validate the username/password
   */
  public BasicAuthValidator(CredentialValidator credentialValidator, AuthTokenManager authTokenManager) {
    super();
    this.credentialValidator = credentialValidator;
    this.authTokenManager = authTokenManager;
  }

  /**
   * Interface that this validator delegates for determining if the credentials are valid.
   */
  interface CredentialValidator {
    Optional<String> validate(String username, String password) throws Exception;
  }

  /**
   * Interface that this validator delegates for generating and validating a token.
   */
  interface AuthTokenManager {
    Optional<String> generateToken(String username, String password);

    Optional<String> validateToken(String token);
  }

  @Override
  public AuthResult validateIncomingHeaders(CallHeaders incomingHeaders) throws Exception {
    AuthResult authResult = parseAndValidateBearerHeaders(incomingHeaders);
    if (authResult != null) {
      return authResult;
    }
    return parseAndValidateBasicHeaders(incomingHeaders);
  }

  /**
   * Appends an authorization header with a bearer token to the outgoing headers.
   * @param outgoingHeaders Outgoing headers to append the authorization header to.
   * @param token Token to be appended to the outgoing header along with the authorization header.
   */
  private void appendAuthHeaderWithBearerTokenToOutgoingHeaders(CallHeaders outgoingHeaders, String token) {
    if (null == AuthUtilities.getValueFromAuthHeader(outgoingHeaders, Auth2Constants.BEARER_PREFIX)) {
      outgoingHeaders.insert(Auth2Constants.AUTHORIZATION_HEADER,
              Auth2Constants.BEARER_PREFIX + token);
    }
  }

  /**
   * Parses and validates basic headers and returns the AuthResult.
   * @param incomingHeaders Incoming header to parse.
   * @return An instance of an AuthResult.
   * @throws Exception When the basic header does not contain the credentials or the credential validation fails.
   */
  private AuthResult parseAndValidateBasicHeaders(CallHeaders incomingHeaders) throws Exception {
    final String authEncoded = AuthUtilities.getValueFromAuthHeader(incomingHeaders, Auth2Constants.BASIC_PREFIX);
    if (authEncoded == null) {
      throw CallStatus.UNAUTHENTICATED.toRuntimeException();
    }
    // The value has the format Base64(<username>:<password>)
    final String authDecoded = new String(Base64.getDecoder().decode(authEncoded), StandardCharsets.UTF_8);
    final int colonPos = authDecoded.indexOf(':');
    if (colonPos == -1) {
      throw CallStatus.UNAUTHORIZED.toRuntimeException();
    }

    final String user = authDecoded.substring(0, colonPos);
    final String password = authDecoded.substring(colonPos + 1);
    credentialValidator.validate(user, password);
    return new AuthResult() {
      @Override
      public String getPeerIdentity() {
        return user;
      }

      @Override
      public void appendToOutgoingHeaders(CallHeaders outgoingHeaders) {
        appendAuthHeaderWithBearerTokenToOutgoingHeaders(outgoingHeaders,
                authTokenManager.generateToken(user, password).get());
      }
    };
  }

  /**
   * Parses and validates bearer headers and returns the AuthResult.
   * @param incomingHeaders Incoming header to parse.
   * @return An instance of an AuthResult.
   * @throws Exception When the bearer header does not contain the token or the token validation fails.
   */
  private AuthResult parseAndValidateBearerHeaders(CallHeaders incomingHeaders) {
    final String parsedValue = AuthUtilities.getValueFromAuthHeader(incomingHeaders, Auth2Constants.BEARER_PREFIX);
    if (parsedValue != null) {
      final Optional<String> peerIdentity = authTokenManager.validateToken(parsedValue);
      if (!peerIdentity.isPresent()) {
        throw CallStatus.UNAUTHORIZED.toRuntimeException();
      }
      return new AuthResult() {
        @Override
        public String getPeerIdentity() {
          return peerIdentity.get();
        }

        @Override
        public void appendToOutgoingHeaders(CallHeaders outgoingHeaders) {
          appendAuthHeaderWithBearerTokenToOutgoingHeaders(outgoingHeaders, parsedValue);
        }
      };
    }
    return null;
  }
}
