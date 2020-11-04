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

import java.io.IOException;

import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallInfo;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.Criteria;
import org.apache.arrow.flight.ErrorFlightMetadata;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.FlightServerMiddleware;
import org.apache.arrow.flight.FlightTestUtil;
import org.apache.arrow.flight.NoOpFlightProducer;
import org.apache.arrow.flight.RequestContext;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Tests for correct handling of cookies from the FlightClient using {@link ClientCookieMiddleware}.
 */
public class TestCookieHandling {
  private static final String SET_COOKIE_HEADER = "Set-Cookie";
  private static final String COOKIE_HEADER = "Cookie";
  private BufferAllocator allocator;
  private FlightServer server;
  private FlightClient client;

  private ClientCookieMiddleware.Factory factory = new ClientCookieMiddleware.Factory();
  private ClientCookieMiddleware cookieMiddleware = new ClientCookieMiddleware(factory);

  @Before
  public void setup() throws Exception {
    allocator = new RootAllocator(Long.MAX_VALUE);
    startServerAndClient();
  }

  @After
  public void cleanup() throws Exception {
    cookieMiddleware = new ClientCookieMiddleware(factory);
    AutoCloseables.close(client, server, allocator);
    client = null;
    server = null;
    allocator = null;
  }

  @Test
  public void basicCookie() {
    CallHeaders headersToSend = new ErrorFlightMetadata();
    headersToSend.insert(SET_COOKIE_HEADER, "k=v");
    cookieMiddleware.onHeadersReceived(headersToSend);
    Assert.assertEquals("k=v", cookieMiddleware.getValidCookiesAsString());
  }

  @Test
  public void cookieStaysAfterMultipleRequests() {
    CallHeaders headersToSend = new ErrorFlightMetadata();
    headersToSend.insert(SET_COOKIE_HEADER, "k=v");
    cookieMiddleware.onHeadersReceived(headersToSend);
    Assert.assertEquals("k=v", cookieMiddleware.getValidCookiesAsString());

    headersToSend = new ErrorFlightMetadata();
    cookieMiddleware.onHeadersReceived(headersToSend);
    Assert.assertEquals("k=v", cookieMiddleware.getValidCookiesAsString());

    headersToSend = new ErrorFlightMetadata();
    cookieMiddleware.onHeadersReceived(headersToSend);
    Assert.assertEquals("k=v", cookieMiddleware.getValidCookiesAsString());
  }

  @Test
  public void cookieAutoExpires() {
    CallHeaders headersToSend = new ErrorFlightMetadata();
    headersToSend.insert(SET_COOKIE_HEADER, "k=v; Max-Age=2");
    cookieMiddleware.onHeadersReceived(headersToSend);
    // Note: using max-age changes cookie version from 0->1, which quotes values.
    Assert.assertEquals("k=\"v\"", cookieMiddleware.getValidCookiesAsString());

    headersToSend = new ErrorFlightMetadata();
    cookieMiddleware.onHeadersReceived(headersToSend);
    Assert.assertEquals("k=\"v\"", cookieMiddleware.getValidCookiesAsString());

    try {
      Thread.sleep(5000);
    } catch (InterruptedException ignored) {
    }

    // Verify that the k cookie was discarded because it expired.
    Assert.assertTrue(cookieMiddleware.getValidCookiesAsString().isEmpty());
  }

  @Test
  public void cookieExplicitlyExpires() {
    CallHeaders headersToSend = new ErrorFlightMetadata();
    headersToSend.insert(SET_COOKIE_HEADER, "k=v; Max-Age=2");
    cookieMiddleware.onHeadersReceived(headersToSend);
    // Note: using max-age changes cookie version from 0->1, which quotes values.
    Assert.assertEquals("k=\"v\"", cookieMiddleware.getValidCookiesAsString());

    headersToSend = new ErrorFlightMetadata();
    headersToSend.insert(SET_COOKIE_HEADER, "k=v; Max-Age=-2");
    cookieMiddleware.onHeadersReceived(headersToSend);

    // Verify that the k cookie was discarded because the server told the client it is expired.
    Assert.assertTrue(cookieMiddleware.getValidCookiesAsString().isEmpty());
  }

  @Ignore
  @Test
  public void cookieExplicitlyExpiresWithMaxAgeMinusOne() {
    CallHeaders headersToSend = new ErrorFlightMetadata();
    headersToSend.insert(SET_COOKIE_HEADER, "k=v; Max-Age=2");
    cookieMiddleware.onHeadersReceived(headersToSend);
    // Note: using max-age changes cookie version from 0->1, which quotes values.
    Assert.assertEquals("k=\"v\"", cookieMiddleware.getValidCookiesAsString());

    headersToSend = new ErrorFlightMetadata();

    // The Java HttpCookie class has a bug where it uses a -1 maxAge to indicate
    // a persistent cookie, when the RFC spec says this should mean the cookie expires immediately.
    headersToSend.insert(SET_COOKIE_HEADER, "k=v; Max-Age=-1");
    cookieMiddleware.onHeadersReceived(headersToSend);

    // Verify that the k cookie was discarded because the server told the client it is expired.
    Assert.assertTrue(cookieMiddleware.getValidCookiesAsString().isEmpty());
  }

  @Test
  public void changeCookieValue() {
    CallHeaders headersToSend = new ErrorFlightMetadata();
    headersToSend.insert(SET_COOKIE_HEADER, "k=v");
    cookieMiddleware.onHeadersReceived(headersToSend);
    Assert.assertEquals("k=v", cookieMiddleware.getValidCookiesAsString());

    headersToSend = new ErrorFlightMetadata();
    headersToSend.insert(SET_COOKIE_HEADER, "k=v2");
    cookieMiddleware.onHeadersReceived(headersToSend);
    Assert.assertEquals("k=v2", cookieMiddleware.getValidCookiesAsString());
  }

  @Test
  public void multipleCookiesWithSetCookie() {
    CallHeaders headersToSend = new ErrorFlightMetadata();
    headersToSend.insert(SET_COOKIE_HEADER, "firstKey=firstVal");
    headersToSend.insert(SET_COOKIE_HEADER, "secondKey=secondVal");
    cookieMiddleware.onHeadersReceived(headersToSend);
    Assert.assertEquals("firstKey=firstVal; secondKey=secondVal", cookieMiddleware.getValidCookiesAsString());
  }

  @Test
  public void cookieStaysAfterMultipleRequestsEndToEnd() {
    client.handshake();
    Assert.assertEquals("k=v", factory.getClientCookieMiddleware().getValidCookiesAsString());
    client.handshake();
    Assert.assertEquals("k=v", factory.getClientCookieMiddleware().getValidCookiesAsString());
    client.listFlights(Criteria.ALL);
    Assert.assertEquals("k=v", factory.getClientCookieMiddleware().getValidCookiesAsString());
  }

  /**
   * A server middleware component that injects SET_COOKIE_HEADER into the outgoing headers.
   */
  static class SetCookieHeaderInjector implements FlightServerMiddleware {
    private final Factory factory;

    public SetCookieHeaderInjector(Factory factory) {
      this.factory = factory;
    }

    @Override
    public void onBeforeSendingHeaders(CallHeaders outgoingHeaders) {
      if (!factory.receivedCookieHeader) {
        outgoingHeaders.insert(SET_COOKIE_HEADER, "k=v");
      }
    }

    @Override
    public void onCallCompleted(CallStatus status) {

    }

    @Override
    public void onCallErrored(Throwable err) {

    }

    static class Factory implements FlightServerMiddleware.Factory<SetCookieHeaderInjector> {
      private boolean receivedCookieHeader = false;

      @Override
      public SetCookieHeaderInjector onCallStarted(CallInfo info, CallHeaders incomingHeaders,
                                                   RequestContext context) {
        if (null != incomingHeaders.get(COOKIE_HEADER)) {
          receivedCookieHeader = true;
        }
        return new SetCookieHeaderInjector(this);
      }
    }
  }

  private void startServerAndClient() throws IOException {
    final FlightProducer flightProducer = new NoOpFlightProducer() {
      public void listFlights(CallContext context, Criteria criteria,
                              StreamListener<FlightInfo> listener) {
        listener.onCompleted();
      }
    };

    this.server = FlightTestUtil.getStartedServer((location) -> FlightServer
            .builder(allocator, location, flightProducer)
            .middleware(FlightServerMiddleware.Key.of("test"), new SetCookieHeaderInjector.Factory())
            .build());

    this.client = FlightClient.builder(allocator, server.getLocation())
            .intercept(factory)
            .build();
  }
}
