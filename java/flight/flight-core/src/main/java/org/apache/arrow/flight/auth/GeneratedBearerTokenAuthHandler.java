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

import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

/**
 * Generates and caches bearer tokens from user credentials.
 */
public class GeneratedBearerTokenAuthHandler extends BearerTokenAuthHandler {
  private final Cache<String, String> bearerToIdentityCache = CacheBuilder.newBuilder()
      .expireAfterAccess(2, TimeUnit.HOURS)
      .build();

  @Override
  protected String getIdentityForBearerToken(String bearerToken) {
    return bearerToIdentityCache.getIfPresent(bearerToken);
  }

  @Override
  public boolean validateBearer(String bearerToken) {
    return bearerToIdentityCache.getIfPresent(bearerToken) != null;
  }

  String registerBearer(AuthResult handshakeResult) {
    final String bearerToken = handshakeResult.getBearerToken()
        .orElseGet(() -> {
          final UUID uuid = UUID.randomUUID();
          final ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[16]);
          byteBuffer.putLong(uuid.getMostSignificantBits());
          byteBuffer.putLong(uuid.getLeastSignificantBits());
          return Base64.getEncoder().encodeToString(byteBuffer.array());
        });
    bearerToIdentityCache.put(bearerToken, handshakeResult.getPeerIdentity());
    return bearerToken;
  }
}
