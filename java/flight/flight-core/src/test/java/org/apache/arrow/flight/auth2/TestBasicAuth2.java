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

package org.apache.arrow.flight.auth2;

import static org.apache.arrow.flight.FlightTestUtil.LOCALHOST;
import static org.apache.arrow.flight.Location.forGrpcInsecure;

import java.io.IOException;

import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.Criteria;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightProducer;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;

public class TestBasicAuth2 {

  private static final String USERNAME_1 = "flight1";
  private static final String USERNAME_2 = "flight2";
  private static final String NO_USERNAME = "";
  private static final String PASSWORD_1 = "woohoo1";
  private static final String PASSWORD_2 = "woohoo2";
  private BufferAllocator allocator;
  private FlightServer server;
  private FlightClient client;
  private FlightClient client2;

  @BeforeEach
  public void setup() throws Exception {
    allocator = new RootAllocator(Long.MAX_VALUE);
    startServerAndClient();
  }

  private FlightProducer getFlightProducer() {
    return new NoOpFlightProducer() {
      @Override
      public void listFlights(CallContext context, Criteria criteria,
          StreamListener<FlightInfo> listener) {
        if (!context.peerIdentity().equals(USERNAME_1) && !context.peerIdentity().equals(USERNAME_2)) {
          listener.onError(new IllegalArgumentException("Invalid username"));
          return;
        }
        listener.onCompleted();
      }

      @Override
      public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
        if (!context.peerIdentity().equals(USERNAME_1) && !context.peerIdentity().equals(USERNAME_2)) {
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
    };
  }

  private void startServerAndClient() throws IOException {
    final FlightProducer flightProducer = getFlightProducer();
    this.server = FlightServer
        .builder(allocator, forGrpcInsecure(LOCALHOST, 0), flightProducer)
        .headerAuthenticator(new GeneratedBearerTokenAuthenticator(
            new BasicCallHeaderAuthenticator(this::validate)))
        .build().start();
    this.client = FlightClient.builder(allocator, server.getLocation())
        .build();
  }

  @AfterEach
  public void shutdown() throws Exception {
    AutoCloseables.close(client, client2, server, allocator);
    client = null;
    client2 = null;
    server = null;
    allocator = null;
  }

  private void startClient2() throws IOException {
    client2 = FlightClient.builder(allocator, server.getLocation())
        .build();
  }

  private CallHeaderAuthenticator.AuthResult validate(String username, String password) {
    if (Strings.isNullOrEmpty(username)) {
      throw CallStatus.UNAUTHENTICATED.withDescription("Credentials not supplied.").toRuntimeException();
    }
    final String identity;
    if (USERNAME_1.equals(username) && PASSWORD_1.equals(password)) {
      identity = USERNAME_1;
    } else if (USERNAME_2.equals(username) && PASSWORD_2.equals(password)) {
      identity = USERNAME_2;
    } else {
      throw CallStatus.UNAUTHENTICATED.withDescription("Username or password is invalid.").toRuntimeException();
    }
    return () -> identity;
  }

  @Test
  public void validAuthWithBearerAuthServer() throws IOException {
    testValidAuth(client);
  }

  @Test
  public void validAuthWithMultipleClientsWithSameCredentialsWithBearerAuthServer() throws IOException {
    startClient2();
    testValidAuthWithMultipleClientsWithSameCredentials(client, client2);
  }

  @Test
  public void validAuthWithMultipleClientsWithDifferentCredentialsWithBearerAuthServer() throws IOException {
    startClient2();
    testValidAuthWithMultipleClientsWithDifferentCredentials(client, client2);
  }

  // ARROW-7722: this test occasionally leaks memory
  @Disabled
  @Test
  public void asyncCall() throws Exception {
    final CredentialCallOption bearerToken = client
        .authenticateBasicToken(USERNAME_1, PASSWORD_1).get();
    client.listFlights(Criteria.ALL, bearerToken);
    try (final FlightStream s = client.getStream(new Ticket(new byte[1]))) {
      while (s.next()) {
        Assertions.assertEquals(4095, s.getRoot().getRowCount());
      }
    }
  }

  @Test
  public void invalidAuthWithBearerAuthServer() throws IOException {
    testInvalidAuth(client);
  }

  @Test
  public void didntAuthWithBearerAuthServer() throws IOException {
    didntAuth(client);
  }

  private void testValidAuth(FlightClient client) {
    final CredentialCallOption bearerToken = client
            .authenticateBasicToken(USERNAME_1, PASSWORD_1).get();
    Assertions.assertTrue(ImmutableList.copyOf(client
            .listFlights(Criteria.ALL, bearerToken))
            .isEmpty());
  }

  private void testValidAuthWithMultipleClientsWithSameCredentials(
          FlightClient client1, FlightClient client2) {
    final CredentialCallOption bearerToken1 = client1
            .authenticateBasicToken(USERNAME_1, PASSWORD_1).get();
    final CredentialCallOption bearerToken2 = client2
            .authenticateBasicToken(USERNAME_1, PASSWORD_1).get();
    Assertions.assertTrue(ImmutableList.copyOf(client1
            .listFlights(Criteria.ALL, bearerToken1))
            .isEmpty());
    Assertions.assertTrue(ImmutableList.copyOf(client2
            .listFlights(Criteria.ALL, bearerToken2))
            .isEmpty());
  }

  private void testValidAuthWithMultipleClientsWithDifferentCredentials(
          FlightClient client1, FlightClient client2) {
    final CredentialCallOption bearerToken1 = client1
            .authenticateBasicToken(USERNAME_1, PASSWORD_1).get();
    final CredentialCallOption bearerToken2 = client2
            .authenticateBasicToken(USERNAME_2, PASSWORD_2).get();
    Assertions.assertTrue(ImmutableList.copyOf(client1
            .listFlights(Criteria.ALL, bearerToken1))
            .isEmpty());
    Assertions.assertTrue(ImmutableList.copyOf(client2
            .listFlights(Criteria.ALL, bearerToken2))
            .isEmpty());
  }

  private void testInvalidAuth(FlightClient client) {
    FlightTestUtil.assertCode(FlightStatusCode.UNAUTHENTICATED, () ->
            client.authenticateBasicToken(USERNAME_1, "WRONG"));

    FlightTestUtil.assertCode(FlightStatusCode.UNAUTHENTICATED, () ->
            client.authenticateBasicToken(NO_USERNAME, PASSWORD_1));

    FlightTestUtil.assertCode(FlightStatusCode.UNAUTHENTICATED, () ->
            client.listFlights(Criteria.ALL).forEach(action -> Assertions.fail()));
  }

  private void didntAuth(FlightClient client) {
    FlightTestUtil.assertCode(FlightStatusCode.UNAUTHENTICATED, () ->
            client.listFlights(Criteria.ALL).forEach(action -> Assertions.fail()));
  }
}
