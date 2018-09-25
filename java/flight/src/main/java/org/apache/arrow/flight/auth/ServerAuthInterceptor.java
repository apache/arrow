/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.flight.auth;

import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCall.Listener;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;

public class ServerAuthInterceptor implements ServerInterceptor {

  private final ServerAuthHandler authHandler;

  public ServerAuthInterceptor(ServerAuthHandler authHandler) {
    this.authHandler = authHandler;
  }

  @Override
  public <ReqT, RespT> Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers,
      ServerCallHandler<ReqT, RespT> next) {
    if (
        !call.getMethodDescriptor().getFullMethodName().equals(AuthConstants.HANDSHAKE_DESCRIPTOR_NAME) &&
        !isValid(headers)) {
      call.close(Status.PERMISSION_DENIED, new Metadata());
      // TODO: we should actually terminate here instead of causing an exception below.
      return new NoopServerCallListener<>();
    }

    return next.startCall(call, headers);
  }

  private final boolean isValid(Metadata headers) {
    byte[] token = headers.get(AuthConstants.TOKEN_KEY);
    return authHandler.isValid(token);
  }

  private static class NoopServerCallListener<T> extends ServerCall.Listener<T> {
  }
}
