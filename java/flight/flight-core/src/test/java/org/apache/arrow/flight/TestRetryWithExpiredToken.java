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

import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.arrow.flight.auth2.Auth2Constants;
import org.apache.arrow.flight.auth2.AuthUtilities;
import org.apache.arrow.flight.auth2.BasicCallHeaderAuthenticator;
import org.apache.arrow.flight.auth2.BearerTokenAuthenticator;
import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;
import org.apache.arrow.flight.impl.Flight;
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
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;

public class TestRetryWithExpiredToken {

  private static final String USERNAME_1 = "flight1";
  private static final String PASSWORD_1 = "woohoo1";
  private BufferAllocator allocator;
  private FlightServer server;
  private FlightClient client;
  private FlightClient client2;

  @Before
  public void setup() throws Exception {
    allocator = new RootAllocator(Long.MAX_VALUE);
    startServerAndClient();
  }

  private FlightProducer getFlightProducer() {
    return new NoOpFlightProducer() {
      @Override
      public void listFlights(CallContext context, Criteria criteria,
                              StreamListener<FlightInfo> listener) {
        if (criteria.getExpression().length > 0) {
          // Don't send anything if criteria are set
          listener.onCompleted();
        }
        try {
          listener.onNext(new FlightInfo(Flight.FlightInfo.newBuilder()
                  .setFlightDescriptor(Flight.FlightDescriptor.newBuilder()
                          .setType(Flight.FlightDescriptor.DescriptorType.CMD)
                          .setCmd(ByteString.copyFrom("flight1", Charsets.UTF_8)))
                  .build()));
        } catch (URISyntaxException e) {
          listener.onError(e);
          return;
        }
        listener.onCompleted();
      }

      @Override
      public void listActions(CallContext context,
                              StreamListener<ActionType> listener) {
        listener.onNext(new ActionType(Flight.ActionType.newBuilder()
                .setDescription("action description1")
                .setType("action type1")
                .build()));
        listener.onNext(new ActionType(Flight.ActionType.newBuilder()
                .setDescription("action description2")
                .setType("action type2")
                .build()));
        listener.onCompleted();
      }

      @Override
      public void doAction(CallContext context, Action action,
                           StreamListener<Result> listener) {
        listener.onNext(new Result("action1".getBytes(Charsets.UTF_8)));
        listener.onNext(new Result("action2".getBytes(Charsets.UTF_8)));
        listener.onCompleted();
      }

      @Override
      public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
        if (!context.peerIdentity().equals(USERNAME_1)) {
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
    this.server = FlightTestUtil.getStartedServer((location) -> FlightServer
            .builder(allocator, location, flightProducer)
            .headerAuthenticator(new GeneratedTestBearerTokenAuthenticator(
                    new BasicCallHeaderAuthenticator(this::validate)))
            .build());

    this.client = FlightClient.builder(allocator, server.getLocation())
            .build();
  }

  @After
  public void shutdown() throws Exception {
    AutoCloseables.close(client, client2, server, allocator);
    client = null;
    client2 = null;
    server = null;
    allocator = null;
  }

  private CallHeaderAuthenticator.AuthResult validate(String username, String password) {
    if (Strings.isNullOrEmpty(username)) {
      throw CallStatus.UNAUTHENTICATED.withDescription("Credentials not supplied.").toRuntimeException();
    }
    final String identity;
    if (USERNAME_1.equals(username) && PASSWORD_1.equals(password)) {
      identity = USERNAME_1;
    } else {
      throw CallStatus.UNAUTHENTICATED.withDescription("Username or password is invalid.").toRuntimeException();
    }
    return () -> identity;
  }

  @Test
  public void testListFlightsWithRetry() {
    client.authenticateBasicToken(USERNAME_1, PASSWORD_1);
    Iterable<FlightInfo> flights = client.listFlights(Criteria.ALL);
    int count = 0;
    for (FlightInfo flight : flights) {
      count += 1;
      Assert.assertArrayEquals(flight.getDescriptor().getCommand(), "flight1".getBytes(Charsets.UTF_8));
    }
    Assert.assertEquals(1, count);
  }

  @Test
  public void testListActionsWithRetry() {
    client.authenticateBasicToken(USERNAME_1, PASSWORD_1);
    Assert.assertFalse(ImmutableList.copyOf(client
            .listActions())
            .isEmpty());
    Assert.assertFalse(ImmutableList.copyOf(client
            .listActions())
            .isEmpty());
  }

  @Test
  public void testDoActionWithRetry() {
    client.authenticateBasicToken(USERNAME_1, PASSWORD_1);
    Assert.assertFalse(ImmutableList.copyOf(client
            .doAction(new Action("hello")))
            .isEmpty());
    Assert.assertFalse(ImmutableList.copyOf(client
            .doAction(new Action("world")))
            .isEmpty());
  }


  /**
   * Generates and caches bearer tokens from user credentials.
   */
  public static class GeneratedTestBearerTokenAuthenticator extends BearerTokenAuthenticator {
    private int counter = 0;

    public GeneratedTestBearerTokenAuthenticator(CallHeaderAuthenticator authenticator) {
      super(authenticator);
    }

    @Override
    protected AuthResult getAuthResultWithBearerToken(AuthResult authResult) {
      counter++;
      return new AuthResult() {
        @Override
        public String getPeerIdentity() {
          return authResult.getPeerIdentity();
        }

        @Override
        public void appendToOutgoingHeaders(CallHeaders outgoingHeaders) {
          authResult.appendToOutgoingHeaders(outgoingHeaders);
          outgoingHeaders.insert(Auth2Constants.AUTHORIZATION_HEADER,
                  Auth2Constants.BEARER_PREFIX + "Token_" + counter);
        }
      };
    }

    @Override
    protected AuthResult validateBearer(String bearerToken) {
      if (bearerToken.equals("Token_1")) {
        throw CallStatus.UNAUTHENTICATED.withDescription("Token expired.").toRuntimeException();
      }
      return new AuthResult() {
        @Override
        public String getPeerIdentity() {
          return USERNAME_1;
        }

        @Override
        public void appendToOutgoingHeaders(CallHeaders outgoingHeaders) {
          if (null == AuthUtilities.getValueFromAuthHeader(outgoingHeaders, Auth2Constants.BEARER_PREFIX)) {
            outgoingHeaders.insert(Auth2Constants.AUTHORIZATION_HEADER, Auth2Constants.BEARER_PREFIX + bearerToken);
          }
        }
      };
    }
  }
}
