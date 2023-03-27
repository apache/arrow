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

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.arrow.driver.jdbc.utils.ArrowFlightConnectionConfigImpl;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.auth2.BasicCallHeaderAuthenticator;
import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;
import org.apache.arrow.flight.auth2.GeneratedBearerTokenAuthenticator;

public class UserPasswordAuthentication implements Authentication {

  private final Map<String, String> validCredentials;

  public UserPasswordAuthentication(Map<String, String> validCredentials) {
    this.validCredentials = validCredentials;
  }

  private String getCredentials(String key) {
    return validCredentials.getOrDefault(key, null);
  }

  @Override
  public CallHeaderAuthenticator authenticate() {
    return new GeneratedBearerTokenAuthenticator(
        new BasicCallHeaderAuthenticator((username, password) -> {
          if (validCredentials.containsKey(username) && getCredentials(username).equals(password)) {
            return () -> username;
          }
          throw CallStatus.UNAUTHENTICATED.withDescription("Invalid credentials.").toRuntimeException();
        }));
  }

  @Override
  public void populateProperties(Properties properties) {
    validCredentials.forEach((key, value) -> {
      properties.put(ArrowFlightConnectionConfigImpl.ArrowFlightConnectionProperty.USER.camelName(), key);
      properties.put(ArrowFlightConnectionConfigImpl.ArrowFlightConnectionProperty.PASSWORD.camelName(), value);
    });
  }

  public static class Builder {
    Map<String, String> credentials = new HashMap<>();

    public Builder user(String username, String password) {
      credentials.put(username, password);
      return this;
    }

    public UserPasswordAuthentication build() {
      return new UserPasswordAuthentication(credentials);
    }
  }
}
