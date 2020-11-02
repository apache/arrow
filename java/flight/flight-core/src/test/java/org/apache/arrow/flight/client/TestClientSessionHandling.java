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

package org.apache.arrow.flight.client;

import static org.junit.Assert.assertEquals;

import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.ErrorFlightMetadata;
import org.apache.arrow.flight.FlightRuntimeException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class TestClientSessionHandling {
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  private static final String SESSION_HEADER_KEY = "SET-SESSION";

  @Test
  public void testSimpleOnHeadersReceived() {
    // Setup
    final ClientSessionWriter writer = new ClientSessionWriter();
    final ClientSessionMiddleware middleware = new ClientSessionMiddleware(writer);
    final CallHeaders headers = new ErrorFlightMetadata();
    final String sessionId = "test-session-id";
    headers.insert(SESSION_HEADER_KEY, sessionId);

    // Test
    middleware.onHeadersReceived(headers);
    assertEquals(sessionId, writer.getSessionId());
  }

  @Test
  public void testSimpleOnBeforeSendingHeaders() {
    // Setup
    final ClientSessionWriter writer = new ClientSessionWriter();
    final ClientSessionMiddleware middleware = new ClientSessionMiddleware(writer);
    final CallHeaders headers = new ErrorFlightMetadata();
    final String sessionId = "test-session-id";
    writer.setSessionId(sessionId);

    // Test
    middleware.onBeforeSendingHeaders(headers);
    assertEquals(sessionId, headers.get(SESSION_HEADER_KEY));
  }

  @Test
  public void testSessionIdPersistence() {
    // Setup
    final ClientSessionWriter writer = new ClientSessionWriter();
    final ClientSessionMiddleware middleware = new ClientSessionMiddleware(writer);
    final CallHeaders receivedHeaders = new ErrorFlightMetadata();
    final String sessionId = "test-session-id";
    receivedHeaders.insert(SESSION_HEADER_KEY, sessionId);
    final CallHeaders outgoingHeaders = new ErrorFlightMetadata();

    // Test
    middleware.onHeadersReceived(receivedHeaders);
    assertEquals(sessionId, writer.getSessionId());

    middleware.onBeforeSendingHeaders(outgoingHeaders);
    assertEquals(sessionId, outgoingHeaders.get(SESSION_HEADER_KEY));
  }

  @Test
  public void testReceivingDifferentSessionId() {
    // Setup
    final ClientSessionWriter writer = new ClientSessionWriter();
    final ClientSessionMiddleware middleware = new ClientSessionMiddleware(writer);

    final CallHeaders receivedHeaders = new ErrorFlightMetadata();
    final String sessionId = "test-session-id";
    receivedHeaders.insert(SESSION_HEADER_KEY, sessionId);

    final CallHeaders outgoingHeaders = new ErrorFlightMetadata();

    final CallHeaders receivedHeadersWithFalseId = new ErrorFlightMetadata();
    final String falseSessionId = "test-session-id-false";
    receivedHeadersWithFalseId.insert(SESSION_HEADER_KEY, falseSessionId);

    // Test
    middleware.onHeadersReceived(receivedHeaders);
    assertEquals(sessionId, writer.getSessionId());

    middleware.onBeforeSendingHeaders(outgoingHeaders);
    assertEquals(sessionId, outgoingHeaders.get(SESSION_HEADER_KEY));

    thrown.expect(FlightRuntimeException.class);
    middleware.onHeadersReceived(receivedHeadersWithFalseId);
  }
}
