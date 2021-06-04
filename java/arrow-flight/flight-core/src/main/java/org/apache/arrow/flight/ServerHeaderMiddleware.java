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

/**
 * Middleware that's used to extract and pass headers to the server during requests.
 */
public class ServerHeaderMiddleware implements FlightServerMiddleware {
  /**
   * Factory for accessing ServerHeaderMiddleware.
   */
  public static class Factory implements FlightServerMiddleware.Factory<ServerHeaderMiddleware> {
    /**
     * Construct a factory for receiving call headers.
     */
    public Factory() {
    }

    @Override
    public ServerHeaderMiddleware onCallStarted(CallInfo callInfo, CallHeaders incomingHeaders,
                                                RequestContext context) {
      return new ServerHeaderMiddleware(incomingHeaders);
    }
  }

  private final CallHeaders headers;

  private ServerHeaderMiddleware(CallHeaders incomingHeaders) {
    this.headers = incomingHeaders;
  }

  /**
   * Retrieve the headers for this call.
   */
  public CallHeaders headers() {
    return headers;
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
