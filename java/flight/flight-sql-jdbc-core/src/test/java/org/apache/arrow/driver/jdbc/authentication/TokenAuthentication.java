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

package org.apache.arrow.driver.jdbc.authentication;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.arrow.driver.jdbc.utils.ArrowFlightConnectionConfigImpl;
import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;

public class TokenAuthentication implements Authentication {
  private final List<String> validCredentials;

  public TokenAuthentication(List<String> validCredentials) {
    this.validCredentials = validCredentials;
  }

  @Override
  public CallHeaderAuthenticator authenticate() {
    return new CallHeaderAuthenticator() {
      @Override
      public AuthResult authenticate(CallHeaders incomingHeaders) {
        String authorization = incomingHeaders.get("authorization");
        if (!validCredentials.contains(authorization)) {
          throw CallStatus.UNAUTHENTICATED.withDescription("Invalid credentials.").toRuntimeException();
        }
        return new AuthResult() {
          @Override
          public String getPeerIdentity() {
            return authorization;
          }
        };
      }
    };
  }

  @Override
  public void populateProperties(Properties properties) {
    this.validCredentials.forEach(value -> properties.put(
        ArrowFlightConnectionConfigImpl.ArrowFlightConnectionProperty.TOKEN.camelName(), value));
  }

  public static final class Builder {
    private final List<String> tokenList = new ArrayList<>();

    public TokenAuthentication.Builder token(String token) {
      tokenList.add("Bearer " + token);
      return this;
    }

    public TokenAuthentication build() {
      return new TokenAuthentication(tokenList);
    }
  }
}
