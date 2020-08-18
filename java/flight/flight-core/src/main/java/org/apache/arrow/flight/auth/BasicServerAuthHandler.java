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
import java.util.Arrays;
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
  public HandshakeResult authenticate(CallHeaders headers) {
    final String authEncoded = AuthUtilities.getValueFromAuthHeader(headers, AuthConstants.BASIC_PREFIX);
    if (authEncoded == null) {
      throw new FlightRuntimeException(CallStatus.UNAUTHENTICATED);
    }

    try {
      // The value has the format Base64(<username>:<password>)
      final String authDecoded = new String(Base64.getDecoder().decode(authEncoded), StandardCharsets.UTF_8);
      final String[] authInParts = authDecoded.split(":");
      if (authInParts.length < 2) {
        throw new FlightRuntimeException(CallStatus.UNAUTHORIZED);
      }

      final String user = authInParts[0];
      final String[] passwordParts = Arrays.copyOfRange(authInParts, 1, authInParts.length);
      final String password = String.join(":", passwordParts);
      final Optional<String> bearerToken = authValidator.validateCredentials(user, password);
      return new HandshakeResult() {
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
      throw new FlightRuntimeException(CallStatus.INTERNAL.withCause(ex));
    } catch (FlightRuntimeException ex) {
      throw ex;
    } catch (Exception ex) {
      throw new FlightRuntimeException(CallStatus.UNAUTHORIZED.withCause(ex));
    }
  }

  @Override
  public boolean validateBearer(String bearerToken) {
    return false;
  }

  /**
   * Interface that this handler delegates to forS determining if credentials are valid.
   */
  public interface BasicAuthValidator {

    Optional<String> validateCredentials(String username, String password) throws Exception;

    Optional<String> isValid(String token);
  }
}
