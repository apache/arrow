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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.arrow.flight.grpc.RequestContextAdapter;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

/**
 * Tests for ServerSessionHandler and ServerSessionMiddleware.
 */
public class TestServerSessionHandling {
  private static final String SESSION_HEADER_KEY = "Session";
  private static final String SET_SESSION_HEADER_KEY = "Set-Session";
  private static final String TEST_SESSION = "name=test-session;id=session-id;max-age=100";
  private static final List<String> VALID_PROPERTY_KEYS = ImmutableList.of(
          "name",
          "id",
          "max-age"
  );

  private class SimpleServerSessionHandler implements ServerSessionHandler {
    private String session;

    SimpleServerSessionHandler() {
      this.session = null;
    }

    @Override
    public String beginSession(CallHeaders headers) {
      session = TEST_SESSION;
      return session;
    }

    @Override
    public String getSession(CallHeaders headers) {
      // No session has been set yet
      if (session == null) {
        return null;
      }

      // Expected session header but none is found
      if (!headers.containsKey(SESSION_HEADER_KEY)) {
        throw CallStatus.NOT_FOUND.toRuntimeException();
      }

      // Validate that session header does not contain invalid properties
      final Map<String, String> propertiesMap = new HashMap<>();
      final String[] inSessionProperties = headers.get(SESSION_HEADER_KEY).split(";");
      for (String pair : inSessionProperties) {
        final String[] keyVal = pair.split("=");
        if (!VALID_PROPERTY_KEYS.contains(keyVal[0])) {
          throw CallStatus.INVALID_ARGUMENT.toRuntimeException();
        }
        propertiesMap.put(keyVal[0], keyVal[1]);
      }

      // Validate that session has not expired
      final int maxAge = Integer.parseInt(propertiesMap.get("max-age"));
      if (maxAge == 0) {
        throw CallStatus.UNAUTHENTICATED.toRuntimeException();
      }

      return session;
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
  public void testInitialSessionId() {
    // Setup
    ServerSessionMiddleware.Factory factory =
        new ServerSessionMiddleware.Factory(new SimpleServerSessionHandler());
    CallHeaders headers = new ErrorFlightMetadata();
    onCallStarted(factory, headers);

    // Test
    assertEquals(TEST_SESSION, headers.get(SET_SESSION_HEADER_KEY));
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
    assertEquals(TEST_SESSION, headers.get(SET_SESSION_HEADER_KEY));
    assertEquals(TEST_SESSION, headersToClient.get(SET_SESSION_HEADER_KEY));
  }

  @Test
  public void testClientReuseValidSession() {
    // Setup
    ServerSessionMiddleware.Factory factory =
        new ServerSessionMiddleware.Factory(new SimpleServerSessionHandler());
    CallHeaders initialHeaders = new ErrorFlightMetadata();
    onCallStarted(factory, initialHeaders);

    CallHeaders nextCallHeaders = new ErrorFlightMetadata();
    nextCallHeaders.insert(SESSION_HEADER_KEY, TEST_SESSION);
    onCallStarted(factory, nextCallHeaders);

    // Test
    assertFalse(initialHeaders.containsKey(SESSION_HEADER_KEY));
    assertEquals(TEST_SESSION, initialHeaders.get(SET_SESSION_HEADER_KEY));

    assertFalse(nextCallHeaders.containsKey(SET_SESSION_HEADER_KEY));
    assertEquals(TEST_SESSION, nextCallHeaders.get(SESSION_HEADER_KEY));
  }

  @Test
  public void testClientDoesNotProvideSessionHeader() throws Exception {
    // Setup
    ServerSessionMiddleware.Factory factory =
            new ServerSessionMiddleware.Factory(new SimpleServerSessionHandler());
    CallHeaders initialHeaders = new ErrorFlightMetadata();
    onCallStarted(factory, initialHeaders);

    CallHeaders nextCallHeaders = new ErrorFlightMetadata();

    // Test
    try {
      onCallStarted(factory, nextCallHeaders);
      testFailed();
    } catch (FlightRuntimeException ex) {
      assertEquals(FlightStatusCode.NOT_FOUND, ex.status().code());
    }
  }

  @Test
  public void testClientProvidesSessionWithInvalidProperty() throws Exception {
    // Setup
    ServerSessionMiddleware.Factory factory =
            new ServerSessionMiddleware.Factory(new SimpleServerSessionHandler());
    CallHeaders initialHeaders = new ErrorFlightMetadata();
    onCallStarted(factory, initialHeaders);

    CallHeaders nextCallHeaders = new ErrorFlightMetadata();
    nextCallHeaders.insert(SESSION_HEADER_KEY,
            "name=test-session;id=session-id;max-age=5;invalid-property=invalid-content");

    // Test
    try {
      onCallStarted(factory, nextCallHeaders);
      testFailed();
    } catch (FlightRuntimeException ex) {
      assertEquals(FlightStatusCode.INVALID_ARGUMENT, ex.status().code());
    }
  }

  @Test
  public void testClientProvidesExpiredSession() throws Exception {
    // Setup
    ServerSessionMiddleware.Factory factory =
            new ServerSessionMiddleware.Factory(new SimpleServerSessionHandler());
    CallHeaders initialHeaders = new ErrorFlightMetadata();
    onCallStarted(factory, initialHeaders);

    CallHeaders nextCallHeaders = new ErrorFlightMetadata();
    nextCallHeaders.insert(SESSION_HEADER_KEY, "name=test-session;id=session-id;max-age=0");

    // Test
    try {
      onCallStarted(factory, nextCallHeaders);
      testFailed();
    } catch (FlightRuntimeException ex) {
      assertEquals(FlightStatusCode.UNAUTHENTICATED, ex.status().code());
    }
  }
}
