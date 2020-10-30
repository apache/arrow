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

import static org.apache.arrow.flight.auth2.BasicAuthValidator.createCompositeCredentialValidator;

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
import org.apache.arrow.flight.auth2.BasicAuthValidator.CompositeCredentialValidator.BasicCredentialValidator;
import org.apache.arrow.flight.auth2.BasicAuthValidator.CompositeCredentialValidator.BearerTokenManager;
import org.apache.arrow.flight.auth2.BasicCallHeaderAuthenticator.ServerAuthValidator;
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

public class TestBasicAuth2 {

  private static final String USERNAME_1 = "flight1";
  private static final String USERNAME_2 = "flight2";
  private static final String NO_USERNAME = "";
  private static final String PASSWORD_1 = "woohoo1";
  private static final String PASSWORD_2 = "woohoo2";
  private static final String VALID_TOKEN_1 = "my_token1";
  private static final String VALID_TOKEN_2 = "my_token2";
  private BufferAllocator allocator;
  private ServerAuthValidator validator;

  @Test
  public void validAuthWithBasicAuthServer() throws IOException {
    final BasicCallHeaderAuthenticator basicCallHeaderAuthenticator = new BasicCallHeaderAuthenticator(validator);
    final FlightClient clientWithBasicAuthServer = getFlightClient(
            basicCallHeaderAuthenticator, new ClientBearerHeaderHandler());
    testValidAuth(clientWithBasicAuthServer);
  }

  @Test
  public void validAuthWithBearerAuthServer() throws IOException {
    final GeneratedBearerTokenAuthHandler generatedBearerTokenAuthHandler =
            new GeneratedBearerTokenAuthHandler(new BasicCallHeaderAuthenticator(validator));
    final FlightClient clientWithBearerAuthServer = getFlightClient(
            generatedBearerTokenAuthHandler, new ClientBearerHeaderHandler());
    testValidAuth(clientWithBearerAuthServer);
  }

  @Test
  public void validAuthWithMultipleClientsWithSameCredentialsWithBasicAuthServer() throws IOException {
    final BasicCallHeaderAuthenticator basicCallHeaderAuthenticator = new BasicCallHeaderAuthenticator(validator);
    final FlightClient clientWithBasicAuthServer1 = getFlightClient(
            basicCallHeaderAuthenticator, new ClientBearerHeaderHandler());
    final FlightClient clientWithBasicAuthServer2 = getFlightClient(
            basicCallHeaderAuthenticator, new ClientBearerHeaderHandler());
    testValidAuthWithMultipleClientsWithSameCredentials(
            clientWithBasicAuthServer1, clientWithBasicAuthServer2);
  }

  @Test
  public void validAuthWithMultipleClientsWithSameCredentialsWithBearerAuthServer() throws IOException {
    final GeneratedBearerTokenAuthHandler generatedBearerTokenAuthHandler =
            new GeneratedBearerTokenAuthHandler(new BasicCallHeaderAuthenticator(validator));
    final FlightClient clientWithBearerAuthServer1 = getFlightClient(
            generatedBearerTokenAuthHandler, new ClientBearerHeaderHandler());
    final FlightClient clientWithBearerAuthServer2 = getFlightClient(
            generatedBearerTokenAuthHandler, new ClientBearerHeaderHandler());
    testValidAuthWithMultipleClientsWithSameCredentials(
            clientWithBearerAuthServer1, clientWithBearerAuthServer2);
  }

  @Test
  public void validAuthWithMultipleClientsWithDifferentCredentialsWithBasicAuthServer() throws IOException {
    final BasicCallHeaderAuthenticator basicCallHeaderAuthenticator = new BasicCallHeaderAuthenticator(validator);
    final FlightClient clientWithBasicAuthServer1 = getFlightClient(
            basicCallHeaderAuthenticator, new ClientBearerHeaderHandler());
    final FlightClient clientWithBasicAuthServer2 = getFlightClient(
            basicCallHeaderAuthenticator, new ClientBearerHeaderHandler());
    testValidAuthWithMultipleClientsWithDifferentCredentials(
            clientWithBasicAuthServer1, clientWithBasicAuthServer2);
  }

  @Test
  public void validAuthWithMultipleClientsWithDifferentCredentialsWithBearerAuthServer() throws IOException {
    final GeneratedBearerTokenAuthHandler generatedBearerTokenAuthHandler =
            new GeneratedBearerTokenAuthHandler(new BasicCallHeaderAuthenticator(validator));
    final FlightClient clientWithBearerAuthServer1 = getFlightClient(
            generatedBearerTokenAuthHandler, new ClientBearerHeaderHandler());
    final FlightClient clientWithBearerAuthServer2 = getFlightClient(
            generatedBearerTokenAuthHandler, new ClientBearerHeaderHandler());
    testValidAuthWithMultipleClientsWithDifferentCredentials(
            clientWithBearerAuthServer1, clientWithBearerAuthServer2);
  }

  // ARROW-7722: this test occasionally leaks memory
  @Ignore
  @Test
  public void asyncCall() throws Exception {
    final BasicCallHeaderAuthenticator basicCallHeaderAuthenticator = new BasicCallHeaderAuthenticator(validator);
    final FlightClient clientWithBasicAuthServer1 = getFlightClient(
            basicCallHeaderAuthenticator, new ClientBearerHeaderHandler());
    final CredentialCallOption bearerToken = clientWithBasicAuthServer1
            .authenticateBasicToken(USERNAME_1, PASSWORD_1).get();
    clientWithBasicAuthServer1.listFlights(Criteria.ALL, bearerToken);
    try (final FlightStream s = clientWithBasicAuthServer1.getStream(new Ticket(new byte[1]))) {
      while (s.next()) {
        Assert.assertEquals(4095, s.getRoot().getRowCount());
      }
    }
  }

  @Test
  public void invalidAuthWithBasicAuthServer() throws IOException {
    final BasicCallHeaderAuthenticator basicCallHeaderAuthenticator = new BasicCallHeaderAuthenticator(validator);
    final FlightClient clientWithBasicAuthServer1 = getFlightClient(
            basicCallHeaderAuthenticator, new ClientBearerHeaderHandler());
    testInvalidAuth(clientWithBasicAuthServer1);
  }

  @Test
  public void invalidAuthWithBearerAuthServer() throws IOException {
    final GeneratedBearerTokenAuthHandler generatedBearerTokenAuthHandler =
            new GeneratedBearerTokenAuthHandler(new BasicCallHeaderAuthenticator(validator));
    final FlightClient clientWithBearerAuthServer1 = getFlightClient(
            generatedBearerTokenAuthHandler, new ClientBearerHeaderHandler());
    testInvalidAuth(clientWithBearerAuthServer1);
  }

  @Test
  public void didntAuthWithBasicAuthServer() throws IOException {
    final BasicCallHeaderAuthenticator basicCallHeaderAuthenticator = new BasicCallHeaderAuthenticator(validator);
    final FlightClient clientWithBasicAuthServer1 = getFlightClient(
            basicCallHeaderAuthenticator, new ClientBearerHeaderHandler());
    didntAuth(clientWithBasicAuthServer1);
  }

  @Test
  public void didntAuthWithBearerAuthServer() throws IOException {
    final GeneratedBearerTokenAuthHandler generatedBearerTokenAuthHandler =
            new GeneratedBearerTokenAuthHandler(new BasicCallHeaderAuthenticator(validator));
    final FlightClient clientWithBearerAuthServer1 = getFlightClient(
            generatedBearerTokenAuthHandler, new ClientBearerHeaderHandler());
    didntAuth(clientWithBearerAuthServer1);
  }

  @Before
  public void setup() {
    allocator = new RootAllocator(Long.MAX_VALUE);
    validator = getServerAuthValidator();
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

  private FlightClient getFlightClient(CallHeaderAuthenticator headerAuthenticator,
                                       ClientHeaderHandler clientHeaderHandler) throws IOException {
    final FlightProducer flightProducer = getFlightProducer();
    final FlightServer flightServer = FlightTestUtil.getStartedServer((location) -> FlightServer
            .builder(allocator, location, flightProducer)
            .headerAuthenticator(headerAuthenticator).build());
    return FlightClient.builder(allocator, flightServer.getLocation())
            .headerHandler(clientHeaderHandler)
            .build();
  }

  private ServerAuthValidator getServerAuthValidator() {
    final BasicCredentialValidator credentialValidator = new BasicCredentialValidator() {
      @Override
      public String validateCredentials(String username, String password) throws Exception {
        if (Strings.isNullOrEmpty(username)) {
          throw CallStatus.UNAUTHENTICATED.withDescription("Credentials not supplied.").toRuntimeException();
        }
        if (USERNAME_1.equals(username) && PASSWORD_1.equals(password)) {
          return USERNAME_1;
        } else if (USERNAME_2.equals(username) && PASSWORD_2.equals(password)) {
          return USERNAME_2;
        } else {
          throw CallStatus.UNAUTHENTICATED.withDescription("Username or password is invalid.").toRuntimeException();
        }
      }
    };
    final BearerTokenManager authTokenManager = new BearerTokenManager() {
      @Override
      public String generateToken(String username, String password) {
        if (USERNAME_1.equals(username) && PASSWORD_1.equals(password)) {
          return VALID_TOKEN_1;
        } else {
          return VALID_TOKEN_2;
        }
      }

      @Override
      public String validateToken(String token) throws Exception {
        if (token.equals(VALID_TOKEN_1)) {
          return USERNAME_1;
        } else if (token.equals(VALID_TOKEN_2)) {
          return USERNAME_2;
        } else {
          throw CallStatus.UNAUTHENTICATED.withDescription("Token is invalid.").toRuntimeException();
        }
      }
    };

    return new BasicAuthValidator(createCompositeCredentialValidator(credentialValidator, authTokenManager));
  }

  @After
  public void shutdown() throws Exception {
    AutoCloseables.close(allocator);
  }

  private void testValidAuth(FlightClient client) {
    final CredentialCallOption bearerToken = client
            .authenticateBasicToken(USERNAME_1, PASSWORD_1).get();
    Assert.assertTrue(ImmutableList.copyOf(client
            .listFlights(Criteria.ALL, bearerToken))
            .isEmpty());
  }

  private void testValidAuthWithMultipleClientsWithSameCredentials(
          FlightClient client1, FlightClient client2) {
    final CredentialCallOption bearerToken1 = client1
            .authenticateBasicToken(USERNAME_1, PASSWORD_1).get();
    final CredentialCallOption bearerToken2 = client2
            .authenticateBasicToken(USERNAME_1, PASSWORD_1).get();
    Assert.assertTrue(ImmutableList.copyOf(client1
            .listFlights(Criteria.ALL, bearerToken1))
            .isEmpty());
    Assert.assertTrue(ImmutableList.copyOf(client2
            .listFlights(Criteria.ALL, bearerToken2))
            .isEmpty());
  }

  private void testValidAuthWithMultipleClientsWithDifferentCredentials(
          FlightClient client1, FlightClient client2) {
    final CredentialCallOption bearerToken1 = client1
            .authenticateBasicToken(USERNAME_1, PASSWORD_1).get();
    final CredentialCallOption bearerToken2 = client2
            .authenticateBasicToken(USERNAME_2, PASSWORD_2).get();
    Assert.assertTrue(ImmutableList.copyOf(client1
            .listFlights(Criteria.ALL, bearerToken1))
            .isEmpty());
    Assert.assertTrue(ImmutableList.copyOf(client2
            .listFlights(Criteria.ALL, bearerToken2))
            .isEmpty());
  }

  private void testInvalidAuth(FlightClient client) {
    FlightTestUtil.assertCode(FlightStatusCode.UNAUTHENTICATED, () ->
            client.authenticateBasicToken(USERNAME_1, "WRONG"));

    FlightTestUtil.assertCode(FlightStatusCode.UNAUTHENTICATED, () ->
            client.authenticateBasicToken(NO_USERNAME, PASSWORD_1));

    FlightTestUtil.assertCode(FlightStatusCode.UNAUTHENTICATED, () ->
            client.listFlights(Criteria.ALL).forEach(action -> Assert.fail()));
  }

  private void didntAuth(FlightClient client) {
    FlightTestUtil.assertCode(FlightStatusCode.UNAUTHENTICATED, () ->
            client.listFlights(Criteria.ALL).forEach(action -> Assert.fail()));
  }
}
