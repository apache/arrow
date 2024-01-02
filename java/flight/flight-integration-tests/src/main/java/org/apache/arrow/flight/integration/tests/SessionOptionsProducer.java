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

package org.apache.arrow.flight.integration.tests;

import java.util.*;

import org.apache.arrow.flight.*;
import org.apache.arrow.flight.sql.NoOpFlightSqlProducer;
import org.apache.arrow.memory.BufferAllocator;

/** The server used for testing Sessions.
 * <p>
 * SetSessionOptions(), GetSessionOptions(), and CloseSession() operate on a
 * simple SessionOptionValue store.
 */
final class SessionOptionsProducer extends NoOpFlightSqlProducer {
  private final BufferAllocator allocator;
  private final FlightServerMiddleware.Key<ServerSessionMiddleware> sessionMiddlewareKey;

  SessionOptionsProducer(FlightServerMiddleware.Key<ServerSessionMiddleware> sessionMiddlewareKey) {
    this.sessionMiddlewareKey = sessionMiddlewareKey;
  }

  @Override
  void setSessionOptions(SetSessionOptionsRequest request, CallContext context,
                                 StreamListener<SetSessionOptionsResult> listener) {
    Map<String, SetSessionOptionsResult.Error> errors = new HashMap();

    ServerSessionMiddleware middleware = context.getMiddleware(sessionMiddlewareKey);
    ServerSessionMiddleware.Session session = middleware.getSession();
    for (Map.Entry<String, SessionOptionValue> entry : request.getSessionOptions().entrySet()) {
      // TODO consider adding immutable value collisions, name blacklist, value/value-type whitelists, etc.
      if (entry.getValue().isEmpty()) {
        session.eraseSessionOption(entry.getKey());
        continue;
      }
      session.setSessionOption(entry.getKey(), entry.getValue());
    }

    return new SetSessionOptionsResult(errors);
  }

  @Override
  void getSessionOptions(GetSessionOptionsRequest request, CallContext context,
                                 StreamListener<GetSessionOptionsResult> listener) {
    ServerSessionMiddleware middleware = context.getMiddleware(sessionMiddlewareKey);
    final Map<String, SessionOptionValue> sessionOptions = middleware.getSession().getSessionOptions();
    return new GetSessionOptionsResult(sessionOptions);
  }

  @Override
  void closeSession(CloseSessionRequest request, CallContext context,
                            StreamListener<CloseSessionResult> listener) {
    ServerSessionMiddleware middleware = context.getMiddleware(sessionMiddlewareKey);
    middleware.closeSession();
  }
}