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

import static org.apache.arrow.flight.FlightTestUtil.LOCALHOST;
import static org.apache.arrow.flight.Location.forGrpcInsecure;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Optional;
import org.apache.arrow.flight.Criteria;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.FlightStatusCode;
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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestBasicAuth {

  private static final String USERNAME = "flight";
  private static final String PASSWORD = "woohoo";
  private static final byte[] VALID_TOKEN = "my_token".getBytes(StandardCharsets.UTF_8);

  private FlightClient client;
  private static FlightServer server;
  private static BufferAllocator allocator;

  @Test
  public void validAuth() {
    client.authenticateBasic(USERNAME, PASSWORD);
    assertEquals(0, ImmutableList.copyOf(client.listFlights(Criteria.ALL)).size());
  }

  @Test
  public void asyncCall() throws Exception {
    client.authenticateBasic(USERNAME, PASSWORD);
    client.listFlights(Criteria.ALL);
    try (final FlightStream s = client.getStream(new Ticket(new byte[1]))) {
      while (s.next()) {
        assertEquals(4095, s.getRoot().getRowCount());
      }
    }
  }

  @Test
  public void invalidAuth() {
    FlightTestUtil.assertCode(
        FlightStatusCode.UNAUTHENTICATED,
        () -> {
          client.authenticateBasic(USERNAME, "WRONG");
        });

    FlightTestUtil.assertCode(
        FlightStatusCode.UNAUTHENTICATED,
        () -> {
          client.listFlights(Criteria.ALL).forEach(action -> fail());
        });
  }

  @Test
  public void didntAuth() {
    FlightTestUtil.assertCode(
        FlightStatusCode.UNAUTHENTICATED,
        () -> {
          client.listFlights(Criteria.ALL).forEach(action -> fail());
        });
  }

  @BeforeEach
  public void testSetup() throws IOException {
    client = FlightClient.builder(allocator, server.getLocation()).build();
  }

  @BeforeAll
  public static void setup() throws IOException {
    allocator = new RootAllocator(Long.MAX_VALUE);
    final BasicServerAuthHandler.BasicAuthValidator validator =
        new BasicServerAuthHandler.BasicAuthValidator() {

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

    server =
        FlightServer.builder(
                allocator,
                forGrpcInsecure(LOCALHOST, 0),
                new NoOpFlightProducer() {
                  @Override
                  public void listFlights(
                      CallContext context, Criteria criteria, StreamListener<FlightInfo> listener) {
                    if (!context.peerIdentity().equals(USERNAME)) {
                      listener.onError(new IllegalArgumentException("Invalid username"));
                      return;
                    }
                    listener.onCompleted();
                  }

                  @Override
                  public void getStream(
                      CallContext context, Ticket ticket, ServerStreamListener listener) {
                    if (!context.peerIdentity().equals(USERNAME)) {
                      listener.error(new IllegalArgumentException("Invalid username"));
                      return;
                    }
                    final Schema pojoSchema =
                        new Schema(
                            ImmutableList.of(
                                Field.nullable("a", Types.MinorType.BIGINT.getType())));
                    try (VectorSchemaRoot root = VectorSchemaRoot.create(pojoSchema, allocator)) {
                      listener.start(root);
                      root.allocateNew();
                      root.setRowCount(4095);
                      listener.putNext();
                      listener.completed();
                    }
                  }
                })
            .authHandler(new BasicServerAuthHandler(validator))
            .build()
            .start();
  }

  @AfterEach
  public void tearDown() throws Exception {
    AutoCloseables.close(client);
  }

  @AfterAll
  public static void shutdown() throws Exception {
    AutoCloseables.close(server);

    allocator.getChildAllocators().forEach(BufferAllocator::close);
    AutoCloseables.close(allocator);
  }
}
