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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import org.apache.arrow.flight.grpc.RequestContextAdapter;
import org.junit.Test;

import com.google.common.collect.Iterators;

/**
 * Tests for ServerSessionHandler and ServerSessionMiddleware.
 */
public class TestServerSessionHandling {
  private static final String SESSION_HEADER_KEY = "SET-SESSION";
  private static final String TEST_SESSION_ID = "test-session-id";

  private class SimpleServerSessionHandler implements ServerSessionHandler {
    private String sessionId;

    public SimpleServerSessionHandler() {
      this.sessionId = null;
    }

    @Override
    public String getSessionId() {
      if (sessionId == null) {
        this.sessionId = TEST_SESSION_ID;
      }
      return sessionId;
    }

    @Override
    public boolean isValid(String sessionId) {
      // The sessionId never expires in this simple test implementation.
      // Production implementations should provide expiration validation.
      return sessionId.equals(this.sessionId);
    }
  }

  private ServerSessionMiddleware onCallStarted(ServerSessionMiddleware.Factory factory,
                                                CallHeaders headers) {
    // Use dummy CallInfo and RequestContext object.
    return factory.onCallStarted(new CallInfo(FlightMethod.DO_ACTION),
            headers, new RequestContextAdapter());
  }

  private void testFailed() throws Exception {
    throw new Exception("Test failed, expected exception.");
  }

  @Test
  public void testNoOpSessionHandlerInitial() {
    // Setup
    ServerSessionMiddleware.Factory factory =
        new ServerSessionMiddleware.Factory(ServerSessionHandler.NO_OP);
    CallHeaders headers = new ErrorFlightMetadata();
    onCallStarted(factory, headers);

    // Test
    assertFalse(headers.containsKey(SESSION_HEADER_KEY));
  }

  @Test
  public void testNoOpSessionHandlerHeaderToClient() {
    // Setup
    ServerSessionMiddleware.Factory factory =
        new ServerSessionMiddleware.Factory(ServerSessionHandler.NO_OP);
    CallHeaders headers = new ErrorFlightMetadata();
    ServerSessionMiddleware middleware = onCallStarted(factory, headers);
    CallHeaders headersToClient = new ErrorFlightMetadata();
    middleware.onBeforeSendingHeaders(headersToClient);

    // Test
    assertFalse(headers.containsKey(SESSION_HEADER_KEY));
    assertFalse(headersToClient.containsKey(SESSION_HEADER_KEY));
  }

  @Test
  public void testInitialSessionId() {
    // Setup
    ServerSessionMiddleware.Factory factory =
        new ServerSessionMiddleware.Factory(new SimpleServerSessionHandler());
    CallHeaders headers = new ErrorFlightMetadata();
    onCallStarted(factory, headers);

    // Test
    assertEquals(TEST_SESSION_ID, headers.get(SESSION_HEADER_KEY));
  }

  @Test
  public void testSessionHeaderToClient() {
    // Setup
    ServerSessionMiddleware.Factory factory =
        new ServerSessionMiddleware.Factory(new SimpleServerSessionHandler());
    CallHeaders headers = new ErrorFlightMetadata();
    ServerSessionMiddleware middleware = onCallStarted(factory, headers);
    CallHeaders headersToClient = new ErrorFlightMetadata();
    middleware.onBeforeSendingHeaders(headersToClient);

    // Test
    assertEquals(TEST_SESSION_ID, headers.get(SESSION_HEADER_KEY));
    assertEquals(TEST_SESSION_ID, headersToClient.get(SESSION_HEADER_KEY));
  }

  @Test
  public void testClientReuseValidSessionId() {
    // Setup
    ServerSessionMiddleware.Factory factory =
        new ServerSessionMiddleware.Factory(new SimpleServerSessionHandler());
    CallHeaders initialHeaders = new ErrorFlightMetadata();
    onCallStarted(factory, initialHeaders);

    CallHeaders nextCallHeaders = new ErrorFlightMetadata();
    nextCallHeaders.insert(SESSION_HEADER_KEY, TEST_SESSION_ID);
    onCallStarted(factory, nextCallHeaders);

    // Test
    assertEquals(1, Iterators.size(initialHeaders.getAll(SESSION_HEADER_KEY).iterator()));
    assertEquals(TEST_SESSION_ID, initialHeaders.get(SESSION_HEADER_KEY));
    assertEquals(1, Iterators.size(nextCallHeaders.getAll(SESSION_HEADER_KEY).iterator()));
    assertEquals(TEST_SESSION_ID, nextCallHeaders.get(SESSION_HEADER_KEY));

  }

  @Test
  public void testClientProvideFalseSessionId() throws Exception {
    // Setup
    ServerSessionMiddleware.Factory factory =
        new ServerSessionMiddleware.Factory(new SimpleServerSessionHandler());
    CallHeaders headers = new ErrorFlightMetadata();
    headers.insert(SESSION_HEADER_KEY, "invalid-session-id-from-client");

    // Test
    try {
      onCallStarted(factory, headers);
      testFailed();
    } catch (FlightRuntimeException ex) {
      assertEquals(FlightStatusCode.UNAUTHENTICATED, ex.status().code());
    }
  }
}
