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

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallInfo;
import org.apache.arrow.flight.FlightMethod;

/**
 * Client credentials that use a username and password.
 */
public final class BasicAuthCredentialWriter implements CredentialWriter {

  private final String name;
  private final String password;

  public BasicAuthCredentialWriter(String name, String password) {
    this.name = name;
    this.password = password;
  }

  @Override
  public void writeCredentials(CallInfo info, CallHeaders outputHeaders) {
    // Basic auth header is only sent during the handshake.
    if (info.method() != FlightMethod.HANDSHAKE) {
      // Note: We must call metadataApplier.apply(), even if we are not modifying any
      // headers. If we don't, the request will not get sent.
      return;
    }

    outputHeaders.insert(AuthConstants.AUTHORIZATION_HEADER, AuthConstants.BASIC_PREFIX +
        Base64.getEncoder().encodeToString(String.format("%s:%s", name, password).getBytes(StandardCharsets.UTF_8)));
  }
}
