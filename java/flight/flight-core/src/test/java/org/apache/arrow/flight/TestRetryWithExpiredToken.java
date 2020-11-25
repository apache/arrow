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

import org.apache.arrow.flight.auth2.Auth2Constants;
import org.apache.arrow.flight.auth2.AuthUtilities;
import org.apache.arrow.flight.auth2.BasicCallHeaderAuthenticator;
import org.apache.arrow.flight.auth2.BearerTokenAuthenticator;
import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;
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
import org.junit.Test;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;

public class TestRetryWithExpiredToken {

  private static final String USERNAME_1 = "flight1";
  private static final String USERNAME_2 = "flight2";
  private static final String NO_USERNAME = "";
  private static final String PASSWORD_1 = "woohoo1";
  private static final String PASSWORD_2 = "woohoo2";
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

  private void testValidAuth(FlightClient client) {
    //    GeneratedTestBearerTokenAuthenticator generatedBearerTokenAuthenticatorMock
    //            = spy(GeneratedTestBearerTokenAuthenticator.class);
    //    when(generatedBearerTokenAuthenticatorMock.authenticate(any()))
    //            .thenCallRealMethod()
    //            .thenThrow(CallStatus.TOKEN_EXPIRED.toRuntimeException())
    //            .thenCallRealMethod();

    final CredentialCallOption bearerToken = client
            .authenticateBasicToken(USERNAME_1, PASSWORD_1).get();
    Assert.assertTrue(ImmutableList.copyOf(client
            .listFlights(Criteria.ALL))
            .isEmpty());
    Assert.assertTrue(ImmutableList.copyOf(client
            .listFlights(Criteria.ALL))
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
