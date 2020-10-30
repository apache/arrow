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

  private final CredentialValidator authValidator;

  public BasicCallHeaderAuthenticator(CredentialValidator authValidator) {
    this.authValidator = authValidator;
  }

  @Override
  public AuthResult authenticate(CallHeaders incomingHeaders) {
    try {
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
      return authValidator.validate(user, password);
    } catch (UnsupportedEncodingException ex) {
      // Note: Intentionally discarding the exception cause when reporting back to the client for security purposes.
      logger.error("Authentication failed due to missing encoding.", ex);
      throw CallStatus.INTERNAL.toRuntimeException();
    } catch (FlightRuntimeException ex) {
      throw ex;
    } catch (Exception ex) {
      // Note: Intentionally discarding the exception cause when reporting back to the client for security purposes.
      logger.error("Authentication failed.", ex);
      throw CallStatus.UNAUTHENTICATED.toRuntimeException();
    }
  }

  /**
   * Interface that this handler delegates to for validating the incoming headers.
   */
  public interface CredentialValidator {
    /**
     * Validate the supplied credentials (username/password) and return the peer identity.
     *
     * @param username The username to validate.
     * @param password The password to validate.
     * @return The peer identity if the supplied credentials are valid.
     * @throws Exception If the supplied credentials are not valid.
     */
    AuthResult validate(String username, String password) throws Exception;
  }
}
