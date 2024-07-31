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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/** Middleware for handling Flight SQL Sessions including session cookie handling. */
public class ServerSessionMiddleware implements FlightServerMiddleware {
  Factory factory;
  boolean existingSession;
  private Session session;
  private String closedSessionId = null;

  public static final String sessionCookieName = "arrow_flight_session_id";

  /** Factory for managing and accessing ServerSessionMiddleware. */
  public static class Factory implements FlightServerMiddleware.Factory<ServerSessionMiddleware> {
    private final ConcurrentMap<String, Session> sessionStore = new ConcurrentHashMap<>();
    private final Callable<String> idGenerator;

    /**
     * Construct a factory for ServerSessionMiddleware.
     *
     * <p>Factory manages and accesses persistent sessions based on HTTP cookies.
     *
     * @param idGenerator A Callable returning unique session id Strings.
     */
    public Factory(Callable<String> idGenerator) {
      this.idGenerator = idGenerator;
    }

    private synchronized Session createNewSession() {
      String id;
      try {
        id = idGenerator.call();
      } catch (Exception ignored) {
        // Most impls aren't going to throw so don't make caller handle a nonexistent checked
        // exception
        throw CallStatus.INTERNAL.withDescription("Session creation error").toRuntimeException();
      }

      Session newSession = new Session(id);
      if (sessionStore.putIfAbsent(id, newSession) != null) {
        // Collision, should never happen
        throw CallStatus.INTERNAL.withDescription("Session creation error").toRuntimeException();
      }
      return newSession;
    }

    private void closeSession(String id) {
      if (sessionStore.remove(id) == null) {
        throw CallStatus.NOT_FOUND
            .withDescription("Session id '" + id + "' not found.")
            .toRuntimeException();
      }
    }

    @Override
    public ServerSessionMiddleware onCallStarted(
        CallInfo callInfo, CallHeaders incomingHeaders, RequestContext context) {
      String sessionId = null;

      final Iterable<String> it = incomingHeaders.getAll("cookie");
      if (it != null) {
        findIdCookie:
        for (final String headerValue : it) {
          for (final String cookie : headerValue.split(" ;")) {
            final String[] cookiePair = cookie.split("=");
            if (cookiePair.length != 2) {
              // Soft failure:  Ignore invalid cookie list field
              break;
            }

            if (sessionCookieName.equals(cookiePair[0]) && cookiePair[1].length() > 0) {
              sessionId = cookiePair[1];
              break findIdCookie;
            }
          }
        }
      }

      if (sessionId == null) {
        // No session cookie, create middleware instance without session.
        return new ServerSessionMiddleware(this, incomingHeaders, null);
      }

      Session session = sessionStore.get(sessionId);
      // Cookie provided by caller, but invalid
      if (session == null) {
        // Can't soft-fail/proceed here, clients will get unexpected behaviour without options they
        // thought were set.
        throw CallStatus.NOT_FOUND
            .withDescription("Invalid " + sessionCookieName + " cookie.")
            .toRuntimeException();
      }

      return new ServerSessionMiddleware(this, incomingHeaders, session);
    }
  }

  /** A thread-safe container for named SessionOptionValues. */
  public static class Session {
    public final String id;
    private ConcurrentMap<String, SessionOptionValue> sessionData =
        new ConcurrentHashMap<String, SessionOptionValue>();

    /**
     * Construct a new Session with the given id.
     *
     * @param id The Session's id string, which is used as the session cookie value.
     */
    private Session(String id) {
      this.id = id;
    }

    /** Get session option by name, or null if it does not exist. */
    public SessionOptionValue getSessionOption(String name) {
      return sessionData.get(name);
    }

    /** Get an immutable copy of the session options map. */
    public Map<String, SessionOptionValue> getSessionOptions() {
      return Collections.unmodifiableMap(new HashMap(sessionData));
    }

    /** Set session option by name to given value. */
    public void setSessionOption(String name, SessionOptionValue value) {
      sessionData.put(name, value);
    }

    /** Idempotently remove name from this session. */
    public void eraseSessionOption(String name) {
      sessionData.remove(name);
    }
  }

  private final CallHeaders headers;

  private ServerSessionMiddleware(
      ServerSessionMiddleware.Factory factory, CallHeaders incomingHeaders, Session session) {
    this.factory = factory;
    headers = incomingHeaders;
    this.session = session;
    existingSession = (session != null);
  }

  /**
   * Check if there is an open session associated with this call.
   *
   * @return True iff there is an open session associated with this call.
   */
  public boolean hasSession() {
    return session != null;
  }

  /**
   * Get the existing or new session value map for this call.
   *
   * @return The session option value map, or null in case of an id generation collision.
   */
  public synchronized Session getSession() {
    if (session == null) {
      session = factory.createNewSession();
    }

    return session;
  }

  /**
   * Close the current session.
   *
   * <p>It is an error to call this without a valid session specified via cookie or equivalent.
   */
  public synchronized void closeSession() {
    if (session == null) {
      throw CallStatus.NOT_FOUND
          .withDescription("No session found for the current call.")
          .toRuntimeException();
    }
    factory.closeSession(session.id);
    closedSessionId = session.id;
    session = null;
  }

  public CallHeaders getCallHeaders() {
    return headers;
  }

  @Override
  public void onBeforeSendingHeaders(CallHeaders outgoingHeaders) {
    if (!existingSession && session != null) {
      outgoingHeaders.insert("set-cookie", sessionCookieName + "=" + session.id);
    }
    if (closedSessionId != null) {
      outgoingHeaders.insert(
          "set-cookie", sessionCookieName + "=" + closedSessionId + "; Max-Age=0");
    }
  }

  @Override
  public void onCallCompleted(CallStatus status) {}

  @Override
  public void onCallErrored(Throwable err) {}
}
