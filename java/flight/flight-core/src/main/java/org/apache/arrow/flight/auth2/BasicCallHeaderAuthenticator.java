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
  public AuthResult authenticate(CallHeaders incomingHeaders) {
    try {
      return authValidator.validateIncomingHeaders(incomingHeaders);
    } catch (UnsupportedEncodingException ex) {
      throw CallStatus.INTERNAL.withCause(ex).toRuntimeException();
    } catch (FlightRuntimeException ex) {
      throw ex;
    } catch (Exception ex) {
      throw CallStatus.UNAUTHORIZED.withCause(ex).toRuntimeException();
    }
  }

  /**
   * Interface that this handler delegates to for validating the incoming headers.
   */
  public interface AuthValidator {
    AuthResult validateIncomingHeaders(CallHeaders incomingHeaders) throws Exception;
  }
}
