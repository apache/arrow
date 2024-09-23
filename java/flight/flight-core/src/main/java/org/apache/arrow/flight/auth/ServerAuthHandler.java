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

import java.util.Iterator;
import java.util.Optional;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;

/**
 * Interface for Server side authentication handlers.
 *
 * @deprecated As of 14.0.0. This implements a stateful "login" flow that does not play well with
 *     distributed or stateless systems. It will not be removed, but should not be used. Instead,
 *     see {@link FlightServer.Builder#headerAuthenticator(CallHeaderAuthenticator)} and {@link
 *     CallHeaderAuthenticator}.
 */
@Deprecated
public interface ServerAuthHandler {

  /**
   * Validate the client token provided on each call.
   *
   * @return An empty optional if the client is not authenticated; the peer identity otherwise (may
   *     be the empty string).
   */
  Optional<String> isValid(byte[] token);

  /**
   * Handle the initial handshake with the client.
   *
   * @param outgoing A writer to send messages to the client.
   * @param incoming An iterator of messages from the client.
   * @return true if client is authenticated, false otherwise.
   */
  boolean authenticate(ServerAuthSender outgoing, Iterator<byte[]> incoming);

  /**
   * Interface for a server implementations to send back authentication messages back to the client.
   */
  interface ServerAuthSender {

    void send(byte[] payload);

    void onError(Throwable cause);
  }

  /** An auth handler that does nothing. */
  ServerAuthHandler NO_OP =
      new ServerAuthHandler() {

        @Override
        public Optional<String> isValid(byte[] token) {
          return Optional.of("");
        }

        @Override
        public boolean authenticate(ServerAuthSender outgoing, Iterator<byte[]> incoming) {
          return true;
        }
      };
}
