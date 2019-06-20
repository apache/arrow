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

import java.util.Iterator;
import java.util.Optional;

import org.apache.arrow.flight.auth.ClientAuthHandler;
import org.apache.arrow.flight.auth.ServerAuthHandler;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import org.junit.Test;

public class TestAuth {

  /** An auth handler that does not send messages should not block the server forever. */
  @Test(expected = RuntimeException.class)
  public void noMessages() throws Exception {
    try (final BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
        final FlightServer s = FlightTestUtil
            .getStartedServer(
                location -> FlightServer.builder(allocator, location, new NoOpFlightProducer()).authHandler(
                    new OneshotAuthHandler()).build());
        final FlightClient client = FlightClient.builder(allocator, s.getLocation()).build()) {
      client.authenticate(new ClientAuthHandler() {
        @Override
        public void authenticate(ClientAuthSender outgoing, Iterator<byte[]> incoming) {
        }

        @Override
        public byte[] getCallToken() {
          return new byte[0];
        }
      });
    }
  }

  /** An auth handler that sends an error should not block the server forever. */
  @Test(expected = RuntimeException.class)
  public void clientError() throws Exception {
    try (final BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
        final FlightServer s = FlightTestUtil
            .getStartedServer(
                location -> FlightServer.builder(allocator, location, new NoOpFlightProducer()).authHandler(
                    new OneshotAuthHandler()).build());
        final FlightClient client = FlightClient.builder(allocator, s.getLocation()).build()) {
      client.authenticate(new ClientAuthHandler() {
        @Override
        public void authenticate(ClientAuthSender outgoing, Iterator<byte[]> incoming) {
          outgoing.send(new byte[0]);
          // Ensure the server-side runs
          incoming.next();
          outgoing.onError(new RuntimeException("test"));
        }

        @Override
        public byte[] getCallToken() {
          return new byte[0];
        }
      });
    }
  }

  private static class OneshotAuthHandler implements ServerAuthHandler {

    @Override
    public Optional<String> isValid(byte[] token) {
      return Optional.of("test");
    }

    @Override
    public boolean authenticate(ServerAuthSender outgoing, Iterator<byte[]> incoming) {
      incoming.next();
      outgoing.send(new byte[0]);
      return false;
    }
  }
}
