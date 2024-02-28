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

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.FlightServerMiddleware;
import org.apache.arrow.flight.GetSessionOptionsRequest;
import org.apache.arrow.flight.GetSessionOptionsResult;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.ServerSessionMiddleware;
import org.apache.arrow.flight.SessionOptionValue;
import org.apache.arrow.flight.SessionOptionValueFactory;
import org.apache.arrow.flight.SetSessionOptionsRequest;
import org.apache.arrow.flight.SetSessionOptionsResult;
import org.apache.arrow.flight.client.ClientCookieMiddleware;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.memory.BufferAllocator;

import com.google.common.collect.ImmutableMap;

/**
 * Scenario to exercise Session Options functionality.
 */
final class SessionOptionsScenario implements Scenario {
  private final FlightServerMiddleware.Key<ServerSessionMiddleware> key =
      FlightServerMiddleware.Key.of("sessionmiddleware");

  @Override
  public FlightProducer producer(BufferAllocator allocator, Location location) throws Exception {
    return new SessionOptionsProducer(key);
  }

  @Override
  public void buildServer(FlightServer.Builder builder) {
    AtomicInteger counter = new AtomicInteger(1000);
    builder.middleware(key, new ServerSessionMiddleware.Factory(() -> String.valueOf(counter.getAndIncrement())));
  }

  @Override
  public void client(BufferAllocator allocator, Location location, FlightClient ignored) throws Exception {
    final ClientCookieMiddleware.Factory factory = new ClientCookieMiddleware.Factory();
    try (final FlightClient flightClient = FlightClient.builder(allocator, location).intercept(factory).build()) {
      final FlightSqlClient client = new FlightSqlClient(flightClient);

      // Set
      SetSessionOptionsRequest req1 = new SetSessionOptionsRequest(ImmutableMap.<String, SessionOptionValue>builder()
          .put("foolong", SessionOptionValueFactory.makeSessionOptionValue(123L))
          .put("bardouble", SessionOptionValueFactory.makeSessionOptionValue(456.0))
          .put("lol_invalid", SessionOptionValueFactory.makeSessionOptionValue("this won't get set"))
          .put("key_with_invalid_value", SessionOptionValueFactory.makeSessionOptionValue("lol_invalid"))
          .put("big_ol_string_list", SessionOptionValueFactory.makeSessionOptionValue(
              new String[]{"a", "b", "sea", "dee", " ", "  ", "geee", "(づ｡◕‿‿◕｡)づ"}))
          .build());
      SetSessionOptionsResult res1 = client.setSessionOptions(req1);
      // Some errors
      IntegrationAssertions.assertEquals(ImmutableMap.<String, SetSessionOptionsResult.Error>builder()
            .put("lol_invalid", new SetSessionOptionsResult.Error(SetSessionOptionsResult.ErrorValue.INVALID_NAME))
            .put("key_with_invalid_value", new SetSessionOptionsResult.Error(
                SetSessionOptionsResult.ErrorValue.INVALID_VALUE))
            .build(),
          res1.getErrors());
      // Some set, some omitted due to above errors
      GetSessionOptionsResult res2 = client.getSessionOptions(new GetSessionOptionsRequest());
      IntegrationAssertions.assertEquals(ImmutableMap.<String, SessionOptionValue>builder()
            .put("foolong", SessionOptionValueFactory.makeSessionOptionValue(123L))
            .put("bardouble", SessionOptionValueFactory.makeSessionOptionValue(456.0))
            .put("big_ol_string_list", SessionOptionValueFactory.makeSessionOptionValue(
                new String[]{"a", "b", "sea", "dee", " ", "  ", "geee", "(づ｡◕‿‿◕｡)づ"}))
            .build(),
          res2.getSessionOptions());
      // Update
      client.setSessionOptions(new SetSessionOptionsRequest(ImmutableMap.<String, SessionOptionValue>builder()
          // Delete
          .put("foolong", SessionOptionValueFactory.makeEmptySessionOptionValue())
          // Update
          .put("big_ol_string_list",
              SessionOptionValueFactory.makeSessionOptionValue("a,b,sea,dee, ,  ,geee,(づ｡◕‿‿◕｡)づ"))
          .build()));
      GetSessionOptionsResult res4 = client.getSessionOptions(new GetSessionOptionsRequest());
      IntegrationAssertions.assertEquals(ImmutableMap.<String, SessionOptionValue>builder()
            .put("bardouble", SessionOptionValueFactory.makeSessionOptionValue(456.0))
            .put("big_ol_string_list",
                SessionOptionValueFactory.makeSessionOptionValue("a,b,sea,dee, ,  ,geee,(づ｡◕‿‿◕｡)づ"))
            .build(),
          res4.getSessionOptions());
    }
  }
}
