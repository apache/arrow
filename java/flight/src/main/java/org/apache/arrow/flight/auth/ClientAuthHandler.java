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

/**
 * Implement authentication for Flight on the client side.
 */
public interface ClientAuthHandler {
  /**
   * Handle the initial handshake with the server.
   * @param outgoing A channel to send data to the server.
   * @param incoming An iterator of incoming data from the server.
   */
  void authenticate(ClientAuthSender outgoing, Iterator<byte[]> incoming);

  /**
   * Get the per-call authentication token.
   */
  byte[] getCallToken();

  /**
   * A communication channel to the server during initial connection.
   */
  interface ClientAuthSender {

    void send(byte[] payload);

    void onError(String message, Throwable cause);

  }

}
