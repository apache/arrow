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

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Optional;

import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.NoOpFlightProducer;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.auth.BasicClientAuthHandler;
import org.apache.arrow.flight.auth.BasicServerAuthHandler;
import org.apache.arrow.memory.BufferAllocator;

/**
 * A scenario testing the built-in basic authentication Protobuf.
 */
final class AuthBasicProtoScenario implements Scenario {

  static final String USERNAME = "arrow";
  static final String PASSWORD = "flight";

  @Override
  public FlightProducer producer(BufferAllocator allocator, Location location) {
    return new NoOpFlightProducer() {
      @Override
      public void doAction(CallContext context, Action action, StreamListener<Result> listener) {
        listener.onNext(new Result(context.peerIdentity().getBytes(StandardCharsets.UTF_8)));
        listener.onCompleted();
      }
    };
  }

  @Override
  public void buildServer(FlightServer.Builder builder) {
    builder.authHandler(new BasicServerAuthHandler(new BasicServerAuthHandler.BasicAuthValidator() {
      @Override
      public byte[] getToken(String username, String password) throws Exception {
        if (!USERNAME.equals(username) || !PASSWORD.equals(password)) {
          throw CallStatus.UNAUTHENTICATED.withDescription("Username or password is invalid.").toRuntimeException();
        }
        return ("valid:" + username).getBytes(StandardCharsets.UTF_8);
      }

      @Override
      public Optional<String> isValid(byte[] token) {
        if (token != null) {
          final String credential = new String(token, StandardCharsets.UTF_8);
          if (credential.startsWith("valid:")) {
            return Optional.of(credential.substring(6));
          }
        }
        return Optional.empty();
      }
    }));
  }

  @Override
  public void client(BufferAllocator allocator, Location location, FlightClient client) {
    final FlightRuntimeException e = IntegrationAssertions.assertThrows(FlightRuntimeException.class, () -> {
      client.listActions().forEach(act -> {
      });
    });
    if (!FlightStatusCode.UNAUTHENTICATED.equals(e.status().code())) {
      throw new AssertionError("Expected UNAUTHENTICATED but found " + e.status().code(), e);
    }

    client.authenticate(new BasicClientAuthHandler(USERNAME, PASSWORD));
    final Result result = client.doAction(new Action("")).next();
    if (!USERNAME.equals(new String(result.getBody(), StandardCharsets.UTF_8))) {
      throw new AssertionError("Expected " + USERNAME + " but got " + Arrays.toString(result.getBody()));
    }
  }
}
