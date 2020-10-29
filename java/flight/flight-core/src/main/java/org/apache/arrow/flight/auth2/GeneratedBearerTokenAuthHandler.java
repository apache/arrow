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

package org.apache.arrow.flight.auth2;

import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.grpc.MetadataAdapter;

import com.google.common.base.Strings;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import io.grpc.Metadata;

/**
 * Generates and caches bearer tokens from user credentials.
 */
public class GeneratedBearerTokenAuthHandler extends BearerTokenAuthHandler {
  private final Cache<String, String> bearerToIdentityCache = CacheBuilder.newBuilder()
      .expireAfterAccess(2, TimeUnit.HOURS)
      .build();

  public GeneratedBearerTokenAuthHandler(BasicCallHeaderAuthenticator basicAuthHandler) {
    super(basicAuthHandler);
  }

  @Override
  protected String validateBearer(String bearerToken) throws Exception {
    final String peerIdentity = bearerToIdentityCache.getIfPresent(bearerToken);
    if (peerIdentity == null) {
      throw CallStatus.UNAUTHENTICATED.toRuntimeException();
    }
    return peerIdentity;
  }

  @Override
  protected void registerBearer(AuthResult authResult) {
    // We generate a dummy header and call appendToOutgoingHeaders with it.
    // We then inspect the dummy header and parse the bearer token if present in the header
    // and generate a new bearer token if a bearer token is not present in the header.
    final CallHeaders dummyHeaders = new MetadataAdapter(new Metadata());
    authResult.appendToOutgoingHeaders(dummyHeaders);
    String bearerToken =
            AuthUtilities.getValueFromAuthHeader(dummyHeaders, Auth2Constants.BEARER_PREFIX);
    if (Strings.isNullOrEmpty(bearerToken)) {
      final UUID uuid = UUID.randomUUID();
      final ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[16]);
      byteBuffer.putLong(uuid.getMostSignificantBits());
      byteBuffer.putLong(uuid.getLeastSignificantBits());
      bearerToken = Base64.getEncoder().encodeToString(byteBuffer.array());
    }
    bearerToIdentityCache.put(bearerToken, authResult.getPeerIdentity());
  }
}
