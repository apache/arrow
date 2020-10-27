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

/**
 * An AuthValidator for Token generation and username/password validation.
 */
public class BasicAuthValidator implements BasicCallHeaderAuthenticator.AuthValidator {

  private final CredentialValidator credentialValidator;
  private final AuthTokenManager authTokenManager;

  /**
   * Creates a validator with supplied TokenManager to generate token 
   * and CredentialValidator to validate username/password. 
   * @param authTokenManager TokenManager used to generate tokens.
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
   * Interface that this validator delegates for generating a Token.
   */
  interface AuthTokenManager {
    Optional<String> generateToken(String username, String password) throws Exception;

    Optional<String> validateToken(String token);
  }

  @Override
  public Optional<String> validateCredentials(String username, String password) throws Exception {
    return credentialValidator.validate(username, password);
  }

  @Override
  public Optional<String> getToken(String username, String password) throws Exception {
    return authTokenManager.generateToken(username, password);
  }

  @Override
  public Optional<String> isValid(String token) {
    return authTokenManager.validateToken(token);
  }
}
