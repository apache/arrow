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
import org.apache.arrow.flight.auth2.BasicAuthValidator.CompositeCredentialValidator.BasicCredentialValidator;
import org.apache.arrow.flight.auth2.BasicAuthValidator.CompositeCredentialValidator.BearerTokenManager;
import org.apache.arrow.flight.auth2.BasicCallHeaderAuthenticator.ServerAuthValidator;
import org.apache.arrow.flight.auth2.CallHeaderAuthenticator.AuthResult;

/**
 * An AuthValidator for validating the headers.
 */
public class BasicAuthValidator implements ServerAuthValidator {
  private final CredentialValidator credentialValidator;

  /**
   * Interface that this validator delegates for determining if the credentials are valid.
   */
  interface CredentialValidator {
    /**
     * Parse the headers and validate the credentials present in the headers.
     * @param headers The headers that contain the credentials to validate.
     * @return The AuthResult after credentials are validated.
     * @throws Exception If the supplied credentials in the headers are not valid.
     */
    AuthResult validateCredentials(CallHeaders headers) throws Exception;
  }

  BasicAuthValidator(CredentialValidator credentialValidator) {
    this.credentialValidator = credentialValidator;
  }

  @Override
  public AuthResult validateIncomingHeaders(CallHeaders incomingHeaders) throws Exception {
    return credentialValidator.validateCredentials(incomingHeaders);
  }

  /**
   * Create a composite credential validator with basic and bearer credential validation support.
   * The validator first tries to parse the header for bearer token and falls back to parsing the
   * header for username and password if no bearer token is found.
   * @param basicCredentialValidator The validator to validate basic headers.
   * @param bearerTokenManager The validator to validate bearer headers.
   * @return a CompositeCredentialValidator instance.
   */
  static CredentialValidator createCompositeCredentialValidator(
          BasicCredentialValidator basicCredentialValidator, BearerTokenManager bearerTokenManager) {
    return new CompositeCredentialValidator(basicCredentialValidator, bearerTokenManager);
  }

  /**
   * Credential validator with basic and bearer credential validation support.
   */
  static class CompositeCredentialValidator implements CredentialValidator {
    final BasicCredentialValidator basicCredentialValidator;
    final BearerTokenManager bearerTokenManager;

    /**
     * Interface that this validator delegates for validating basic credentials.
     */
    interface BasicCredentialValidator {
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
    interface BearerTokenManager {
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

    CompositeCredentialValidator(
            BasicCredentialValidator basicCredentialValidator, BearerTokenManager bearerTokenManager) {
      this.basicCredentialValidator = basicCredentialValidator;
      this.bearerTokenManager = bearerTokenManager;
    }

    @Override
    public AuthResult validateCredentials(CallHeaders headers) throws Exception {
      final AuthResult authResult = parseAndValidateBearerHeaders(headers);
      if (authResult != null) {
        return authResult;
      }
      return parseAndValidateBasicHeaders(headers);
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
     * @throws Exception When the basic header does not contain any credentials or the credential validation fails.
     */
    private AuthResult parseAndValidateBasicHeaders(CallHeaders incomingHeaders) throws Exception {
      final String authEncoded = AuthUtilities.getValueFromAuthHeader(
              incomingHeaders, Auth2Constants.BASIC_PREFIX);
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
      final String peerIdentity = basicCredentialValidator.validateCredentials(user, password);
      return new AuthResult() {
        @Override
        public String getPeerIdentity() {
          return peerIdentity;
        }

        @Override
        public void appendToOutgoingHeaders(CallHeaders outgoingHeaders) {
          appendAuthHeaderWithBearerTokenToOutgoingHeaders(outgoingHeaders,
                  bearerTokenManager.generateToken(user, password));
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
      final String parsedValue = AuthUtilities.getValueFromAuthHeader(
              incomingHeaders, Auth2Constants.BEARER_PREFIX);
      if (parsedValue != null) {
        final String peerIdentity = bearerTokenManager.validateToken(parsedValue);
        return new AuthResult() {
          @Override
          public String getPeerIdentity() {
            return peerIdentity;
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
}
