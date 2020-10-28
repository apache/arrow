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

import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.ErrorFlightMetadata;
import org.junit.After;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Tests for correct handling of cookies from the FlightClient using {@link ClientCookieMiddleware}.
 */
public class TestCookieHandling {
  private static final String SET_COOKIE_HEADER = "Set-Cookie";
  private static final String SET_COOKIE2_HEADER = "Set-Cookie2";

  private ClientCookieMiddleware cookieMiddleware = new ClientCookieMiddleware();

  @After
  public void cleanup() {
    cookieMiddleware = new ClientCookieMiddleware();
  }

  @Test
  public void basicCookie() {
    CallHeaders headersToSend = new ErrorFlightMetadata();
    headersToSend.insert(SET_COOKIE_HEADER, "k=v");
    cookieMiddleware.onHeadersReceived(headersToSend);
    Assert.assertEquals("k=v", cookieMiddleware.calculateCookieString());
  }

  @Test
  public void cookieStaysAfterMultipleRequests() {
    CallHeaders headersToSend = new ErrorFlightMetadata();
    headersToSend.insert(SET_COOKIE_HEADER, "k=v");
    cookieMiddleware.onHeadersReceived(headersToSend);
    Assert.assertEquals("k=v", cookieMiddleware.calculateCookieString());

    headersToSend = new ErrorFlightMetadata();
    cookieMiddleware.onHeadersReceived(headersToSend);
    Assert.assertEquals("k=v", cookieMiddleware.calculateCookieString());

    headersToSend = new ErrorFlightMetadata();
    cookieMiddleware.onHeadersReceived(headersToSend);
    Assert.assertEquals("k=v", cookieMiddleware.calculateCookieString());
  }

  @Test
  public void cookieAutoExpires() {
    CallHeaders headersToSend = new ErrorFlightMetadata();
    headersToSend.insert(SET_COOKIE_HEADER, "k=v; Max-Age=2");
    cookieMiddleware.onHeadersReceived(headersToSend);
    // Note: using max-age changes cookie version from 0->1, which quotes values.
    Assert.assertEquals("k=\"v\"", cookieMiddleware.calculateCookieString());

    headersToSend = new ErrorFlightMetadata();
    cookieMiddleware.onHeadersReceived(headersToSend);
    Assert.assertEquals("k=\"v\"", cookieMiddleware.calculateCookieString());

    try {
      Thread.sleep(5000);
    } catch (InterruptedException ignored) {
    }

    // Verify that the k cookie was discarded because it expired.
    Assert.assertTrue(cookieMiddleware.calculateCookieString().isEmpty());
  }

  @Test
  public void cookieExplicitlyExpires() {
    CallHeaders headersToSend = new ErrorFlightMetadata();
    headersToSend.insert(SET_COOKIE_HEADER, "k=v; Max-Age=2");
    cookieMiddleware.onHeadersReceived(headersToSend);
    // Note: using max-age changes cookie version from 0->1, which quotes values.
    Assert.assertEquals("k=\"v\"", cookieMiddleware.calculateCookieString());

    headersToSend = new ErrorFlightMetadata();
    headersToSend.insert(SET_COOKIE_HEADER, "k=v; Max-Age=-2");
    cookieMiddleware.onHeadersReceived(headersToSend);

    // Verify that the k cookie was discarded because the server told the client it is expired.
    Assert.assertTrue(cookieMiddleware.calculateCookieString().isEmpty());
  }

  @Ignore
  @Test
  public void cookieExplicitlyExpiresWithMaxAgeMinusOne() {
    CallHeaders headersToSend = new ErrorFlightMetadata();
    headersToSend.insert(SET_COOKIE_HEADER, "k=v; Max-Age=2");
    cookieMiddleware.onHeadersReceived(headersToSend);
    // Note: using max-age changes cookie version from 0->1, which quotes values.
    Assert.assertEquals("k=\"v\"", cookieMiddleware.calculateCookieString());

    headersToSend = new ErrorFlightMetadata();

    // The Java HttpCookie class has a bug where it uses a -1 maxAge to indicate
    // a persistent cookie, when the RFC spec says this should mean the cookie expires immediately.
    headersToSend.insert(SET_COOKIE_HEADER, "k=v; Max-Age=-1");
    cookieMiddleware.onHeadersReceived(headersToSend);

    // Verify that the k cookie was discarded because the server told the client it is expired.
    Assert.assertTrue(cookieMiddleware.calculateCookieString().isEmpty());
  }

  @Test
  public void changeCookieValue() {
    CallHeaders headersToSend = new ErrorFlightMetadata();
    headersToSend.insert(SET_COOKIE_HEADER, "k=v");
    cookieMiddleware.onHeadersReceived(headersToSend);
    Assert.assertEquals("k=v", cookieMiddleware.calculateCookieString());

    headersToSend = new ErrorFlightMetadata();
    headersToSend.insert(SET_COOKIE_HEADER, "k=v2");
    cookieMiddleware.onHeadersReceived(headersToSend);
    Assert.assertEquals("k=v2", cookieMiddleware.calculateCookieString());
  }

  @Test
  public void multipleCookiesWithSetCookie() {
    CallHeaders headersToSend = new ErrorFlightMetadata();
    headersToSend.insert(SET_COOKIE_HEADER, "firstKey=firstVal");
    headersToSend.insert(SET_COOKIE_HEADER, "secondKey=secondVal");
    cookieMiddleware.onHeadersReceived(headersToSend);
    Assert.assertEquals("firstKey=firstVal; secondKey=secondVal", cookieMiddleware.calculateCookieString());
  }

  @Test
  public void basicCookiesWithSetCookie2() {
    CallHeaders headersToSend = new ErrorFlightMetadata();
    headersToSend.insert(SET_COOKIE2_HEADER, "firstKey=firstVal");
    cookieMiddleware.onHeadersReceived(headersToSend);
    Assert.assertEquals("firstKey=firstVal", cookieMiddleware.calculateCookieString());
  }

  @Ignore
  @Test
  public void multipleCookiesWithSetCookie2() {
    // There seems to be a JDK bug with HttpCookie.parse() with multiple cookies
    // in a Set-Cookie2 header. This is odd, because that method explictly returns a list
    // of cookies because of Set-Cookie2.
    // Set-Cookie2 itself is deprecated.
    CallHeaders headersToSend = new ErrorFlightMetadata();
    headersToSend.insert(SET_COOKIE2_HEADER, "firstKey=firstVal, secondKey=secondVal");
    cookieMiddleware.onHeadersReceived(headersToSend);
    Assert.assertEquals("firstKey=firstVal; secondKey=secondVal", cookieMiddleware.calculateCookieString());
  }
}
