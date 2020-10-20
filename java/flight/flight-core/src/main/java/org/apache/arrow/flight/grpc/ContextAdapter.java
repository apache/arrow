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

package org.apache.arrow.flight.grpc;

import java.util.HashMap;
import java.util.Map;

import org.apache.arrow.flight.RequestContext;
import org.apache.arrow.flight.auth.AuthConstants;

import io.grpc.Context;

/**
 * Adapter class for gRPC contexts.
 */
public class ContextAdapter implements RequestContext {
  // gRPC uses reference equality when looking up keys in a Context. Cache used keys in this static map
  // so that look ups can succeed.
  private static final Map<String, Context.Key<String>> usedKeys = new HashMap<>();
  private static final Context.Key<String> authkey = Context.keyWithDefault(AuthConstants.PEER_IDENTITY_KEY, "");
  private Context context = Context.current();

  /**
   * Retrieves the gRPC context.
   * @return the gRPC context.
   */
  public Context getContext() {
    return context;
  }

  @Override
  public void put(String key, String value) {
    context = context.withValue(authkey, value);
  }

  @Override
  public String get(String key) {
    return authkey.get(context);
  }

  private static Context.Key<String> getGrpcKey(String key) {
    synchronized (usedKeys) {
      Context.Key<String> cachedKey = usedKeys.get(key);
      if (cachedKey == null) {
        cachedKey = Context.keyWithDefault(key, "");
        usedKeys.put(key, cachedKey);
      }
      return cachedKey;
    }
  }
}
