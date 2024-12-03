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

import java.util.Objects;
import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallInfo;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightClientMiddleware;

/** Middleware for capturing and setting bearer tokens. */
public class ClientBearerMiddleware implements FlightClientMiddleware {

  private final Factory factory;

  private ClientBearerMiddleware(Factory factory) {
    this.factory = Objects.requireNonNull(factory);
  }

  @Override
  public void onHeadersReceived(CallHeaders incomingHeaders) {
    final String bearerValue =
        AuthUtilities.getValueFromAuthHeader(incomingHeaders, Auth2Constants.BEARER_PREFIX);
    if (bearerValue != null) {
      factory.setBearerValue(bearerValue);
    }
  }

  @Override
  public void onBeforeSendingHeaders(CallHeaders outgoingHeaders) {
    final String bearerValue = factory.getBearerValue();
    if (bearerValue != null) {
      outgoingHeaders.insert(
          Auth2Constants.AUTHORIZATION_HEADER, Auth2Constants.BEARER_PREFIX + bearerValue);
    }
  }

  @Override
  public void onCallCompleted(CallStatus status) {}

  public static class Factory implements FlightClientMiddleware.Factory {

    private volatile String bearerValue;

    public Factory() {}

    @Override
    public FlightClientMiddleware onCallStarted(CallInfo info) {
      return new ClientBearerMiddleware(this);
    }

    String getBearerValue() {
      return bearerValue;
    }

    void setBearerValue(String bearerValue) {
      // Avoid volatile write if the value hasn't changed
      if (!Objects.equals(this.bearerValue, bearerValue)) {
        this.bearerValue = bearerValue;
      }
    }
  }
}
