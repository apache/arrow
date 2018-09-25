/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.flight.auth;

import java.io.IOException;
import java.util.Arrays;

import org.apache.arrow.flight.Criteria;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.NoOpFlightProducer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.google.common.collect.ImmutableList;

public class TestAuth {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  private static final String USERNAME = "flight";
  private static final String PASSWORD = "woohoo";
  private static final byte[] VALID_TOKEN = "my_token".getBytes();

  private FlightClient client;
  private FlightServer server;
  private BufferAllocator allocator;

  @Test
  public void validAuth() {
    client.authenticateBasic(USERNAME, PASSWORD);
    ImmutableList.copyOf(client.listFlights(Criteria.ALL));
  }

  @Test
  public void invalidAuth() {

    thrown.expectMessage("PERMISSION_DENIED");
    client.authenticateBasic(USERNAME, "WRONG");

    thrown.expectMessage("PERMISSION_DENIED");
    client.listFlights(Criteria.ALL);
  }

  @Test
  public void didntAuth() {
    thrown.expectMessage("PERMISSION_DENIED");
    client.listFlights(Criteria.ALL);
  }

  @Before
  public void setup() throws IOException {
    allocator = new RootAllocator(Long.MAX_VALUE);
    final Location l = new Location("localhost", 12233);

    final BasicServerAuthHandler.BasicAuthValidator validator = new BasicServerAuthHandler.BasicAuthValidator() {

      @Override
      public boolean isValid(byte[] token) {
        return Arrays.equals(token, VALID_TOKEN);
      }

      @Override
      public byte[] getToken(String username, String password) throws Exception {
        if (USERNAME.equals(username) && PASSWORD.equals(password)) {
          return VALID_TOKEN;
        } else {
          throw new IllegalArgumentException("invalid credentials");
        }
      }
    };

    server = new FlightServer(
        allocator,
        l.getPort(),
        new NoOpFlightProducer() {
          @Override
          public void listFlights(Criteria criteria, StreamListener<FlightInfo> listener) {
            listener.onCompleted();
          }
        },
        new BasicServerAuthHandler(validator));

    server.start();
    client = new FlightClient(allocator, l);
  }



  @After
  public void shutdown() throws Exception {
    AutoCloseables.close(client, server, allocator);
  }

}
