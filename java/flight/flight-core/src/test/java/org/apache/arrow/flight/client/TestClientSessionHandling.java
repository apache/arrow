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
import org.junit.Test;

/**
 * Tests for ClientSessionWriter and ClientSessionMiddleware.
 */
public class TestClientSessionHandling {
  private static final String SET_SESSION_HEADER_KEY = "set-session";
  private static final String SESSION_HEADER_KEY = "session";

  @Test
  public void testSimpleOnHeadersReceived() {
    // Setup
    final ClientSessionWriter writer = new ClientSessionWriter();
    final ClientSessionMiddleware middleware = new ClientSessionMiddleware(writer);
    final CallHeaders headers = new ErrorFlightMetadata();
    final String session = "test-session";
    headers.insert(SET_SESSION_HEADER_KEY, session);

    // Test
    middleware.onHeadersReceived(headers);
    assertEquals(session, writer.getSession());
  }

  @Test
  public void testSimpleOnBeforeSendingHeaders() {
    // Setup
    final ClientSessionWriter writer = new ClientSessionWriter();
    final ClientSessionMiddleware middleware = new ClientSessionMiddleware(writer);
    final CallHeaders headers = new ErrorFlightMetadata();
    final String sessionId = "test-session-id";
    writer.setSession(sessionId);

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
    receivedHeaders.insert(SET_SESSION_HEADER_KEY, sessionId);
    final CallHeaders outgoingHeaders = new ErrorFlightMetadata();

    // Test
    middleware.onHeadersReceived(receivedHeaders);
    assertEquals(sessionId, writer.getSession());

    middleware.onBeforeSendingHeaders(outgoingHeaders);
    assertEquals(sessionId, outgoingHeaders.get(SESSION_HEADER_KEY));
  }
}
