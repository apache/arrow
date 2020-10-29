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

package org.apache.arrow.flight;

import java.util.HashMap;
import java.util.Map;

/**
 * Middleware that's used to extract and pass properties to the server during requests.
 */
public class ServerPropertyMiddleware implements FlightServerMiddleware {
  /**
   * Factory for accessing ServerCallPropertyMiddleware.
   */
  public static class Factory implements FlightServerMiddleware.Factory<ServerPropertyMiddleware> {
    private final ServerPropertyHandler propertyHandler;

    /**
     * Construct a factory with the given property handler.
     * @param propertyHandler The property handler that will be used to pass properties.
     */
    public Factory(ServerPropertyHandler propertyHandler) {
      this.propertyHandler = propertyHandler;
    }

    @Override
    public ServerPropertyMiddleware onCallStarted(CallInfo callInfo, CallHeaders incomingHeaders,
                                                  RequestContext context) {
      final Map<String, String> properties = new HashMap<>();
      for (String key : incomingHeaders.keys()) {
        if (key.startsWith(FlightConstants.PROPERTY_PREFIX)) {
          properties.put(key.substring(FlightConstants.PROPERTY_PREFIX.length()), incomingHeaders.get(key));
        }
      }

      if (!properties.isEmpty()) {
        propertyHandler.accept(properties);
      }

      return new ServerPropertyMiddleware();
    }
  }

  public ServerPropertyMiddleware() {
  }

  @Override
  public void onBeforeSendingHeaders(CallHeaders outgoingHeaders) {
  }

  @Override
  public void onCallCompleted(CallStatus status) {
  }

  @Override
  public void onCallErrored(Throwable err) {
  }
}
