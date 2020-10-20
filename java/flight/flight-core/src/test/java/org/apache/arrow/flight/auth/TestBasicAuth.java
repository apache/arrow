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

package org.apache.arrow.flight.auth;

import java.io.IOException;
import java.util.Optional;

import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.Criteria;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.FlightTestUtil;
import org.apache.arrow.flight.NoOpFlightProducer;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.grpc.CredentialCallOption;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;

public class TestBasicAuth {

  private static final String USERNAME = "flight";
  private static final String NO_USERNAME = "";
  private static final String PASSWORD = "woohoo";
  private static final String VALID_TOKEN = "my_token";

  private FlightClient client;
  private FlightServer server;
  private BufferAllocator allocator;

  @Test
  public void validAuth() {
    final CredentialCallOption bearerToken = client.authenticateBasic(USERNAME, PASSWORD).get();
    Assert.assertTrue(ImmutableList.copyOf(client
        .listFlights(Criteria.ALL, bearerToken))
        .isEmpty());
  }

  // ARROW-7722: this test occasionally leaks memory
  @Ignore
  @Test
  public void asyncCall() throws Exception {
    final CredentialCallOption bearerToken = client.authenticateBasic(USERNAME, PASSWORD).get();
    client.listFlights(Criteria.ALL, bearerToken);
    try (final FlightStream s = client.getStream(new Ticket(new byte[1]))) {
      while (s.next()) {
        Assert.assertEquals(4095, s.getRoot().getRowCount());
      }
    }
  }

  @Test
  public void invalidAuth() {
    FlightTestUtil.assertCode(FlightStatusCode.UNAUTHENTICATED, () ->
        client.authenticateBasic(USERNAME, "WRONG"));

    FlightTestUtil.assertCode(FlightStatusCode.UNAUTHENTICATED, () ->
            client.authenticateBasic(NO_USERNAME, PASSWORD));

    FlightTestUtil.assertCode(FlightStatusCode.UNAUTHENTICATED, () ->
        client.listFlights(Criteria.ALL).forEach(action -> Assert.fail()));
  }

  @Test
  public void didntAuth() {
    FlightTestUtil.assertCode(FlightStatusCode.UNAUTHENTICATED, () ->
        client.listFlights(Criteria.ALL).forEach(action -> Assert.fail()));
  }

  @Before
  public void setup() throws IOException {
    allocator = new RootAllocator(Long.MAX_VALUE);
    final BasicServerAuthHandler.BasicAuthValidator validator = new BasicServerAuthHandler.BasicAuthValidator() {

      @Override
      public Optional<String> validateCredentials(String username, String password) throws Exception {
        if (Strings.isNullOrEmpty(username)) {
          throw CallStatus.UNAUTHENTICATED.withDescription("Credentials not supplied").toRuntimeException();
        }

        if (!USERNAME.equals(username) || !PASSWORD.equals(password)) {
          throw CallStatus.UNAUTHENTICATED.withDescription("Username or password is invalid.").toRuntimeException();
        }
        return Optional.of("valid:" + username);
      }

      @Override
      public Optional<String> isValid(String token) {
        if (token.equals(VALID_TOKEN)) {
          return Optional.of(USERNAME);
        }
        return Optional.empty();
      }
    };

    server = FlightTestUtil.getStartedServer((location) -> FlightServer.builder(
        allocator,
        location,
        new NoOpFlightProducer() {
          @Override
          public void listFlights(CallContext context, Criteria criteria,
                                  StreamListener<FlightInfo> listener) {
            if (!context.peerIdentity().equals(USERNAME)) {
              listener.onError(new IllegalArgumentException("Invalid username"));
              return;
            }
            listener.onCompleted();
          }

          @Override
          public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
            if (!context.peerIdentity().equals(USERNAME)) {
              listener.error(new IllegalArgumentException("Invalid username"));
              return;
            }
            final Schema pojoSchema = new Schema(ImmutableList.of(Field.nullable("a",
                    Types.MinorType.BIGINT.getType())));
            try (VectorSchemaRoot root = VectorSchemaRoot.create(pojoSchema, allocator)) {
              listener.start(root);
              root.allocateNew();
              root.setRowCount(4095);
              listener.putNext();
              listener.completed();
            }
          }
        }).authHandler(new BasicServerAuthHandler(validator)).build());
    client = FlightClient.builder(allocator, server.getLocation()).build();
  }

  @After
  public void shutdown() throws Exception {
    AutoCloseables.close(client, server, allocator);
  }
}
