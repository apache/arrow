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

import java.net.HttpCookie;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallInfo;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightClientMiddleware;
import org.apache.arrow.util.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A client middleware for receiving and sending cookie information.
 * Note that this class will not persist permanent cookies beyond the lifetime
 * of this session.
 *
 * This middleware will automatically remove cookies that have expired.
 * <b>Note</b>: Negative max-age values currently do not get marked as expired due to
 * a JDK issue. Use max-age=0 to explicitly remove an existing cookie.
 */
public class ClientCookieMiddleware implements FlightClientMiddleware {
  private static final Logger LOGGER = LoggerFactory.getLogger(ClientCookieMiddleware.class);

  private static final String SET_COOKIE_HEADER = "Set-Cookie";
  private static final String COOKIE_HEADER = "Cookie";

  private final Factory factory;

  @VisibleForTesting
  ClientCookieMiddleware(Factory factory) {
    this.factory = factory;
  }

  /**
   * Factory used within FlightClient.
   */
  public static class Factory implements FlightClientMiddleware.Factory {
    // Use a map to track the most recent version of a cookie from the server.
    // Note that cookie names are case-sensitive (but header names aren't).
    private ConcurrentMap<String, HttpCookie> cookies = new ConcurrentHashMap<>();

    @Override
    public ClientCookieMiddleware onCallStarted(CallInfo info) {
      return new ClientCookieMiddleware(this);
    }

    private void updateCookies(Iterable<String> newCookieHeaderValues) {
      // Note: Intentionally overwrite existing cookie values.
      // A cookie defined once will continue to be used in all subsequent
      // requests on the client instance. The server can send the same cookie again
      // with a different value and the client will use the new value in future requests.
      // The server can also update a cookie to have an Expiry in the past or negative age
      // to signal that the client should stop using the cookie immediately.
      newCookieHeaderValues.forEach(headerValue -> {
        try {
          final List<HttpCookie> parsedCookies = HttpCookie.parse(headerValue);
          parsedCookies.forEach(parsedCookie -> {
            final String cookieNameLc = parsedCookie.getName().toLowerCase(Locale.ENGLISH);
            if (parsedCookie.hasExpired()) {
              cookies.remove(cookieNameLc);
            } else {
              cookies.put(parsedCookie.getName().toLowerCase(Locale.ENGLISH), parsedCookie);
            }
          });
        } catch (IllegalArgumentException ex) {
          LOGGER.warn("Skipping incorrectly formatted Set-Cookie header with value '{}'.", headerValue);
        }
      });
    }
  }

  @Override
  public void onBeforeSendingHeaders(CallHeaders outgoingHeaders) {
    final String cookieValue = getValidCookiesAsString();
    if (!cookieValue.isEmpty()) {
      outgoingHeaders.insert(COOKIE_HEADER, cookieValue);
    }
  }

  @Override
  public void onHeadersReceived(CallHeaders incomingHeaders) {
    final Iterable<String> setCookieHeaders = incomingHeaders.getAll(SET_COOKIE_HEADER);
    if (setCookieHeaders != null) {
      factory.updateCookies(setCookieHeaders);
    }
  }

  @Override
  public void onCallCompleted(CallStatus status) {

  }

  /**
   * Discards expired cookies and returns the valid cookies as a String delimited by ';'.
   */
  @VisibleForTesting
  String getValidCookiesAsString() {
    // Discard expired cookies.
    factory.cookies.entrySet().removeIf(cookieEntry -> cookieEntry.getValue().hasExpired());

    // Cookie header value format:
    // [<cookie-name1>=<cookie-value1>; <cookie-name2>=<cookie-value2; ...]
    return factory.cookies.entrySet().stream()
        .map(cookie -> cookie.getValue().toString())
        .collect(Collectors.joining("; "));
  }
}
