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

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.Executor;

import io.grpc.CallCredentials;
import io.grpc.Metadata;

/**
 * A client auth handler that supports username and password.
 */
public final class BasicAuthCallCredentials extends CallCredentials {

  private final String name;
  private final String password;

  public BasicAuthCallCredentials(String name, String password) {
    this.name = name;
    this.password = password;
  }

  @Override
  public void applyRequestMetadata(RequestInfo requestInfo, Executor executor, MetadataApplier metadataApplier) {
    final Metadata authMetadata = new Metadata();
    // Basic authorization header is
    // Authorization: Basic Base64(username:password)
    final Metadata.Key<String> authorizationKey = Metadata.Key.of("Authorization", Metadata.ASCII_STRING_MARSHALLER);
    authMetadata.put(authorizationKey, "Basic " +
        Base64.getEncoder().encodeToString(String.format("%s:%s", name, password).getBytes(StandardCharsets.UTF_8)));
    metadataApplier.apply(new Metadata());
  }

  @Override
  public void thisUsesUnstableApi() {
    // Mandatory to override this to acknowledge that CallCredentials is Experimental.
  }
}
