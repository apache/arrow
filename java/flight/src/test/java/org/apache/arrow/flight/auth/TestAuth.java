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

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;

import org.apache.arrow.flight.Criteria;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.FlightTestUtil;
import org.apache.arrow.flight.NoOpFlightProducer;
import org.apache.arrow.flight.Ticket;
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

import com.google.common.collect.ImmutableList;

import io.grpc.StatusRuntimeException;

public class TestAuth {
  final String PERMISSION_DENIED = "PERMISSION_DENIED";

  private static final String USERNAME = "flight";
  private static final String PASSWORD = "woohoo";
  private static final byte[] VALID_TOKEN = "my_token".getBytes();

  private FlightClient client;
  private FlightServer server;
  private BufferAllocator allocator;

  @Test
  public void validAuth() {
    client.authenticateBasic(USERNAME, PASSWORD);
    Assert.assertTrue(ImmutableList.copyOf(client.listFlights(Criteria.ALL)).size() >= 0);
  }

  @Test
  public void asyncCall() {
    client.authenticateBasic(USERNAME, PASSWORD);
    client.listFlights(Criteria.ALL);
    FlightStream s = client.getStream(new Ticket(new byte[1]));

    while (s.next()) {
      Assert.assertEquals(4095, s.getRoot().getRowCount());
      s.getRoot().clear();
    }
  }

  @Test
  public void invalidAuth() {
    assertThrows(StatusRuntimeException.class, () -> {
      client.authenticateBasic(USERNAME, "WRONG");
    }, PERMISSION_DENIED);

    assertThrows(StatusRuntimeException.class, () -> {
      client.listFlights(Criteria.ALL);
    }, PERMISSION_DENIED);
  }

  @Test
  public void didntAuth() {
    assertThrows(StatusRuntimeException.class, () -> {
      client.listFlights(Criteria.ALL);
    }, PERMISSION_DENIED);
  }

  @Before
  public void setup() throws IOException {
    allocator = new RootAllocator(Long.MAX_VALUE);
    final BasicServerAuthHandler.BasicAuthValidator validator = new BasicServerAuthHandler.BasicAuthValidator() {

      @Override
      public Optional<String> isValid(byte[] token) {
        if (Arrays.equals(token, VALID_TOKEN)) {
          return Optional.of(USERNAME);
        }
        return Optional.empty();
      }

      @Override
      public byte[] getToken(String username, String password) {
        if (USERNAME.equals(username) && PASSWORD.equals(password)) {
          return VALID_TOKEN;
        } else {
          throw new IllegalArgumentException("invalid credentials");
        }
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
            VectorSchemaRoot root = VectorSchemaRoot.create(pojoSchema, allocator);
            listener.start(root);
            root.allocateNew();
            root.setRowCount(4095);
            listener.putNext();
            root.clear();
            listener.completed();
          }
        }).authHandler(new BasicServerAuthHandler(validator)).build());
    client = FlightClient.builder(allocator, server.getLocation()).build();
  }

  @After
  public void shutdown() throws Exception {
    AutoCloseables.close(client, server, allocator);
  }

}
