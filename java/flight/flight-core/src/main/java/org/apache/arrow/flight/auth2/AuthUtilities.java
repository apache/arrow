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

import org.apache.arrow.flight.CallHeaders;

/**
 * Utility class for completing the auth process.
 */
public final class AuthUtilities {
  private AuthUtilities() {

  }

  /**
   * Helper method for retrieving a value from the Authorization header.
   *
   * @param headers     The headers to inspect.
   * @param valuePrefix The prefix within the value portion of the header to extract away.
   * @return The header value.
   */
  public static String getValueFromAuthHeader(CallHeaders headers, String valuePrefix) {
    final String authHeaderValue = headers.get(Auth2Constants.AUTHORIZATION_HEADER);
    if (authHeaderValue != null) {
      if (authHeaderValue.regionMatches(true, 0, valuePrefix, 0, valuePrefix.length())) {
        return authHeaderValue.substring(valuePrefix.length());
      }
    }
    return null;
  }

}
