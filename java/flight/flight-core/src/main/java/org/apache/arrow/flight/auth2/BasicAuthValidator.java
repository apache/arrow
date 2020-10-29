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
   * @param credentialValidator CredentialValidator used to validate Credentials the username/password
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
    /**
     * Validate the supplied credentials (username/password) and return the peer identity.
     * @param username The username to validate.
     * @param password The password to validate.
     * @return The peer identity if the supplied credentials are valid.
     * @throws Exception If the supplied credentials are not valid.
     */
    String validateCredentials(String username, String password) throws Exception;
  }

  /**
   * Interface that this validator delegates for generating and validating a token.
   */
  interface AuthTokenManager {
    /**
     * Generate a token for the supplied username and password.
     * @param username The username to generate the token for.
     * @param password The password to generate the token for.
     * @return The generated token.
     */
    String generateToken(String username, String password);

    /**
     * Validate the supplied token and return the peer identity.
     * @param token The token to be validated.
     * @return The peer identity if the supplied token is valid.
     * @throws Exception If the supplied token is not valid.
     */
    String validateToken(String token) throws Exception;
  }

  @Override
  public AuthResult validateIncomingHeaders(CallHeaders incomingHeaders) throws Exception {
    final AuthResult authResult = parseAndValidateBearerHeaders(incomingHeaders);
    if (authResult != null) {
      return authResult;
    }
    return parseAndValidateBasicHeaders(incomingHeaders);
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
      throw CallStatus.UNAUTHENTICATED.toRuntimeException();
    }

    final String user = authDecoded.substring(0, colonPos);
    final String password = authDecoded.substring(colonPos + 1);
    final String peerIdentity = credentialValidator.validateCredentials(user, password);
    return new AuthResult() {
      @Override
      public String getPeerIdentity() {
        return peerIdentity;
      }

      @Override
      public CallHeaderAuthenticator.HeaderMetadata getHeaderMetadata() {
        return new CallHeaderAuthenticator.HeaderMetadata() {
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
            return authTokenManager.generateToken(user, password);
          }
        };
      }

      @Override
      public void appendToOutgoingHeaders(CallHeaders outgoingHeaders) {
        final CallHeaderAuthenticator.HeaderMetadata metadata = this.getHeaderMetadata();
        if (null == AuthUtilities.getValueFromAuthHeader(outgoingHeaders, metadata.getValuePrefix())) {
          outgoingHeaders.insert(metadata.getKey(), metadata.getValuePrefix() + metadata.getValue());
        }
      }
    };
  }

  /**
   * Parses and validates bearer headers and returns the AuthResult.
   * @param incomingHeaders Incoming header to parse.
   * @return An instance of an AuthResult.
   * @throws Exception When the bearer header does not contain the token or the token validation fails.
   */
  private AuthResult parseAndValidateBearerHeaders(CallHeaders incomingHeaders) throws Exception {
    final String parsedValue = AuthUtilities.getValueFromAuthHeader(incomingHeaders, Auth2Constants.BEARER_PREFIX);
    if (parsedValue != null) {
      final String peerIdentity = authTokenManager.validateToken(parsedValue);
      return new AuthResult() {
        @Override
        public String getPeerIdentity() {
          return peerIdentity;
        }

        public CallHeaderAuthenticator.HeaderMetadata getHeaderMetadata() {
          return new CallHeaderAuthenticator.HeaderMetadata() {
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
              return parsedValue;
            }
          };
        }

        @Override
        public void appendToOutgoingHeaders(CallHeaders outgoingHeaders) {
          final CallHeaderAuthenticator.HeaderMetadata metadata = this.getHeaderMetadata();
          if (null == AuthUtilities.getValueFromAuthHeader(outgoingHeaders, metadata.getValuePrefix())) {
            outgoingHeaders.insert(metadata.getKey(), metadata.getValuePrefix() + metadata.getValue());
          }
        }
      };
    }
    return null;
  }
}
