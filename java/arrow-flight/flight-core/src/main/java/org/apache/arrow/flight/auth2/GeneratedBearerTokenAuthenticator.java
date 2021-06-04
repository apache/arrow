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
public class GeneratedBearerTokenAuthenticator extends BearerTokenAuthenticator {
  private final Cache<String, String> bearerToIdentityCache;

  /**
   * Generate bearer tokens for the given basic call authenticator.
   * @param authenticator The authenticator to initial validate inputs with.
   */
  public GeneratedBearerTokenAuthenticator(CallHeaderAuthenticator authenticator) {
    this(authenticator, CacheBuilder.newBuilder().expireAfterAccess(2, TimeUnit.HOURS));
  }

  /**
   * Generate bearer tokens for the given basic call authenticator.
   * @param authenticator The authenticator to initial validate inputs with.
   * @param timeoutMinutes The time before tokens expire after being accessed.
   */
  public GeneratedBearerTokenAuthenticator(CallHeaderAuthenticator authenticator, int timeoutMinutes) {
    this(authenticator, CacheBuilder.newBuilder().expireAfterAccess(timeoutMinutes, TimeUnit.MINUTES));
  }

  /**
   * Generate bearer tokens for the given basic call authenticator.
   * @param authenticator The authenticator to initial validate inputs with.
   * @param cacheBuilder The configuration of the cache of bearer tokens.
   */
  public GeneratedBearerTokenAuthenticator(CallHeaderAuthenticator authenticator,
      CacheBuilder<Object, Object> cacheBuilder) {
    super(authenticator);
    bearerToIdentityCache = cacheBuilder.build();
  }

  @Override
  protected AuthResult validateBearer(String bearerToken) {
    final String peerIdentity = bearerToIdentityCache.getIfPresent(bearerToken);
    if (peerIdentity == null) {
      throw CallStatus.UNAUTHENTICATED.toRuntimeException();
    }

    return new AuthResult() {
      @Override
      public String getPeerIdentity() {
        return peerIdentity;
      }

      @Override
      public void appendToOutgoingHeaders(CallHeaders outgoingHeaders) {
        if (null == AuthUtilities.getValueFromAuthHeader(outgoingHeaders, Auth2Constants.BEARER_PREFIX)) {
          outgoingHeaders.insert(Auth2Constants.AUTHORIZATION_HEADER, Auth2Constants.BEARER_PREFIX + bearerToken);
        }
      }
    };
  }

  @Override
  protected AuthResult getAuthResultWithBearerToken(AuthResult authResult) {
    // We generate a dummy header and call appendToOutgoingHeaders with it.
    // We then inspect the dummy header and parse the bearer token if present in the header
    // and generate a new bearer token if a bearer token is not present in the header.
    final CallHeaders dummyHeaders = new MetadataAdapter(new Metadata());
    authResult.appendToOutgoingHeaders(dummyHeaders);
    String bearerToken =
            AuthUtilities.getValueFromAuthHeader(dummyHeaders, Auth2Constants.BEARER_PREFIX);
    final AuthResult authResultWithBearerToken;
    if (Strings.isNullOrEmpty(bearerToken)) {
      // Generate a new bearer token and return an AuthResult that can write it.
      final UUID uuid = UUID.randomUUID();
      final ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[16]);
      byteBuffer.putLong(uuid.getMostSignificantBits());
      byteBuffer.putLong(uuid.getLeastSignificantBits());
      final String newToken = Base64.getEncoder().encodeToString(byteBuffer.array());
      bearerToken = newToken;
      authResultWithBearerToken = new AuthResult() {
        @Override
        public String getPeerIdentity() {
          return authResult.getPeerIdentity();
        }

        @Override
        public void appendToOutgoingHeaders(CallHeaders outgoingHeaders) {
          authResult.appendToOutgoingHeaders(outgoingHeaders);
          outgoingHeaders.insert(Auth2Constants.AUTHORIZATION_HEADER, Auth2Constants.BEARER_PREFIX + newToken);
        }
      };
    } else {
      // Use the bearer token supplied by the original auth result.
      authResultWithBearerToken = authResult;
    }
    bearerToIdentityCache.put(bearerToken, authResult.getPeerIdentity());
    return authResultWithBearerToken;
  }
}
