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

import java.util.HashMap;
import java.util.Map;

import org.apache.arrow.flight.CloseSessionRequest;
import org.apache.arrow.flight.CloseSessionResult;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightServerMiddleware;
import org.apache.arrow.flight.GetSessionOptionsRequest;
import org.apache.arrow.flight.GetSessionOptionsResult;
import org.apache.arrow.flight.ServerSessionMiddleware;
import org.apache.arrow.flight.SessionOptionValue;
import org.apache.arrow.flight.SessionOptionValueFactory;
import org.apache.arrow.flight.SetSessionOptionsRequest;
import org.apache.arrow.flight.SetSessionOptionsResult;
import org.apache.arrow.flight.sql.NoOpFlightSqlProducer;

/** The server used for testing Sessions.
 * <p>
 * SetSessionOptions(), GetSessionOptions(), and CloseSession() operate on a
 * simple SessionOptionValue store.
 */
final class SessionOptionsProducer extends NoOpFlightSqlProducer {
  private static final SessionOptionValue invalidOptionValue =
      SessionOptionValueFactory.makeSessionOptionValue("lol_invalid");
  private final FlightServerMiddleware.Key<ServerSessionMiddleware> sessionMiddlewareKey;

  SessionOptionsProducer(FlightServerMiddleware.Key<ServerSessionMiddleware> sessionMiddlewareKey) {
    this.sessionMiddlewareKey = sessionMiddlewareKey;
  }

  @Override
  public void setSessionOptions(SetSessionOptionsRequest request, CallContext context,
                         StreamListener<SetSessionOptionsResult> listener) {
    Map<String, SetSessionOptionsResult.Error> errors = new HashMap();

    ServerSessionMiddleware middleware = context.getMiddleware(sessionMiddlewareKey);
    ServerSessionMiddleware.Session session = middleware.getSession();
    for (Map.Entry<String, SessionOptionValue> entry : request.getSessionOptions().entrySet()) {
      // Blacklisted option name
      if (entry.getKey().equals("lol_invalid")) {
        errors.put(entry.getKey(),
            new SetSessionOptionsResult.Error(SetSessionOptionsResult.ErrorValue.INVALID_NAME));
        continue;
      }
      // Blacklisted option value
      // Recommend using a visitor to check polymorphic equality, but this check is easy
      if (entry.getValue().equals(invalidOptionValue)) {
        errors.put(entry.getKey(),
            new SetSessionOptionsResult.Error(SetSessionOptionsResult.ErrorValue.INVALID_VALUE));
        continue;
      }
      // Business as usual:
      if (entry.getValue().isEmpty()) {
        session.eraseSessionOption(entry.getKey());
        continue;
      }
      session.setSessionOption(entry.getKey(), entry.getValue());
    }
    listener.onNext(new SetSessionOptionsResult(errors));
    listener.onCompleted();
  }

  @Override
  public void getSessionOptions(GetSessionOptionsRequest request, CallContext context,
                         StreamListener<GetSessionOptionsResult> listener) {
    ServerSessionMiddleware middleware = context.getMiddleware(sessionMiddlewareKey);
    final Map<String, SessionOptionValue> sessionOptions = middleware.getSession().getSessionOptions();
    listener.onNext(new GetSessionOptionsResult(sessionOptions));
    listener.onCompleted();
  }

  @Override
  public void closeSession(CloseSessionRequest request, CallContext context,
                    StreamListener<CloseSessionResult> listener) {
    ServerSessionMiddleware middleware = context.getMiddleware(sessionMiddlewareKey);
    try {
      middleware.closeSession();
    } catch (FlightRuntimeException fre) {
      listener.onError(fre);
      return;
    }
    listener.onNext(new CloseSessionResult(CloseSessionResult.Status.CLOSED));
    listener.onCompleted();
  }
}
