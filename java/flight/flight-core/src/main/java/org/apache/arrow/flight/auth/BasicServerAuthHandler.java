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

package org.apache.arrow.flight.auth;

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
public class BasicServerAuthHandler implements ServerAuthHandler {

  private static final Logger logger = LoggerFactory.getLogger(BasicServerAuthHandler.class);
  private final BasicAuthValidator authValidator;

  public BasicServerAuthHandler(BasicAuthValidator authValidator) {
    super();
    this.authValidator = authValidator;
  }

  @Override
  public AuthResult authenticate(CallHeaders headers) {
    final String authEncoded = AuthUtilities.getValueFromAuthHeader(headers, AuthConstants.BASIC_PREFIX);
    if (authEncoded == null) {
      throw CallStatus.UNAUTHENTICATED.toRuntimeException();
    }

    try {
      // The value has the format Base64(<username>:<password>)
      final String authDecoded = new String(Base64.getDecoder().decode(authEncoded), StandardCharsets.UTF_8);
      final int colonPos = authDecoded.indexOf(':');
      if (colonPos == -1) {
        throw CallStatus.UNAUTHORIZED.toRuntimeException();
      }

      final String user = authDecoded.substring(0, colonPos);
      final String password = authDecoded.substring(colonPos + 1);
      final Optional<String> bearerToken = authValidator.validateCredentials(user, password);
      return new AuthResult() {
        @Override
        public String getPeerIdentity() {
          return user;
        }

        @Override
        public Optional<String> getBearerToken() {
          return bearerToken;
        }
      };

    } catch (UnsupportedEncodingException ex) {
      throw CallStatus.INTERNAL.withCause(ex).toRuntimeException();
    } catch (FlightRuntimeException ex) {
      throw ex;
    } catch (Exception ex) {
      throw CallStatus.UNAUTHORIZED.withCause(ex).toRuntimeException();
    }
  }

  @Override
  public boolean validateBearer(String bearerToken) {
    return false;
  }

  @Override
  public boolean enableCachedCredentials() {
    return true;
  }

  /**
   * Interface that this handler delegates to for determining if credentials are valid.
   */
  public interface BasicAuthValidator {

    Optional<String> validateCredentials(String username, String password) throws Exception;

    Optional<String> isValid(String token);
  }
}
