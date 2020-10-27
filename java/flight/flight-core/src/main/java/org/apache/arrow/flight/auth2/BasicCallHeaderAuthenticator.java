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

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Optional;

import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A ServerAuthHandler for username/password authentication.
 */
public class BasicCallHeaderAuthenticator implements CallHeaderAuthenticator {

  private static final Logger logger = LoggerFactory.getLogger(BasicCallHeaderAuthenticator.class);
  private final AuthValidator authValidator;

  public BasicCallHeaderAuthenticator(AuthValidator authValidator) {
    super();
    this.authValidator = authValidator;
  }

  @Override
  public AuthResult authenticate(CallHeaders headers) {
    try {
      AuthResult authResult = parseAndValidateNonBasicHeaders(headers);
      if (authResult != null) {
        return authResult;
      }
      return parseAndValidateBasicHeaders(headers);
    } catch (UnsupportedEncodingException ex) {
      throw CallStatus.INTERNAL.withCause(ex).toRuntimeException();
    } catch (FlightRuntimeException ex) {
      throw ex;
    } catch (Exception ex) {
      throw CallStatus.UNAUTHORIZED.withCause(ex).toRuntimeException();
    }
  }

  private AuthResult parseAndValidateNonBasicHeaders(CallHeaders headers) {
    final String parsedValue = authValidator.parseNonBasicHeaders(headers);
    if (parsedValue != null) {
      final Optional<String> peerIdentity = authValidator.isValid(parsedValue);
      if (!peerIdentity.isPresent()) {
        throw CallStatus.UNAUTHORIZED.toRuntimeException();
      }
      return new AuthResult() {
        @Override
        public String getPeerIdentity() {
          return peerIdentity.get();
        }

        @Override
        public Optional<String> getBearerToken() {
          return Optional.of(parsedValue);
        }

        @Override
        public void appendToOutgoingHeaders(CallHeaders outgoingHeaders) {
          // Don't append anything to the outgoing headers
        }
      };
    }
    return null;
  }

  private AuthResult parseAndValidateBasicHeaders(CallHeaders headers) throws Exception {
    final String authEncoded = AuthUtilities.getValueFromAuthHeader(headers, Auth2Constants.BASIC_PREFIX);
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
    authValidator.validateCredentials(user, password);
    final Optional<String> bearerToken = authValidator.getToken(user, password);
    return new AuthResult() {
      @Override
      public String getPeerIdentity() {
        return user;
      }

      @Override
      public Optional<String> getBearerToken() {
        return bearerToken;
      }

      @Override
      public void appendToOutgoingHeaders(CallHeaders outgoingHeaders) {
        authValidator.appendToOutgoingHeaders(outgoingHeaders, user, password);
      }
    };
  }

  @Override
  public boolean validateBearer(String bearerToken) {
    return false;
  }

  /**
   * Interface that this handler delegates to for determining if credentials are valid
   * and generating tokens.
   */
  public interface AuthValidator {

    Optional<String> validateCredentials(String username, String password) throws Exception;

    Optional<String> getToken(String username, String password) throws Exception;

    Optional<String> isValid(String token);

    String parseNonBasicHeaders(CallHeaders incomingHeaders);

    void appendToOutgoingHeaders(CallHeaders outgoingHeaders, String username, String password);
  }
}
