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

import org.apache.arrow.flight.impl.Flight.BasicAuth;

/**
 * A client auth handler that supports username and password.
 */
public class BasicClientAuthHandler implements ClientAuthHandler {

  private final String name;
  private final String password;

  public BasicClientAuthHandler(String name, String password) {
    this.name = name;
    this.password = password;
  }

  @Override
  public byte[] authenticate(ClientAuthSender outgoing, Iterator<byte[]> incoming) {
    BasicAuth.Builder builder = BasicAuth.newBuilder();
    if (name != null) {
      builder.setUsername(name);
    }

    if (password != null) {
      builder.setPassword(password);
    }

    outgoing.send(builder.build().toByteArray());
    return incoming.next();
  }

}
