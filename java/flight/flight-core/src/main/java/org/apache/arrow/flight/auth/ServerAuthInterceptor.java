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

import java.util.Optional;

import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.grpc.StatusUtils;

import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCall.Listener;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

/**
 * GRPC Interceptor for performing authentication.
 */
public class ServerAuthInterceptor implements ServerInterceptor {

  private final ServerAuthHandler authHandler;

  public ServerAuthInterceptor(ServerAuthHandler authHandler) {
    this.authHandler = authHandler;
  }

  @Override
  public <ReqT, RespT> Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers,
      ServerCallHandler<ReqT, RespT> next) {
    if (!call.getMethodDescriptor().getFullMethodName().equals(AuthConstants.HANDSHAKE_DESCRIPTOR_NAME)) {
      final Optional<String> peerIdentity;

      // Allow customizing the response code by throwing FlightRuntimeException
      try {
        peerIdentity = isValid(headers);
      } catch (FlightRuntimeException e) {
        final Status grpcStatus = StatusUtils.toGrpcStatus(e.status());
        call.close(grpcStatus, new Metadata());
        return new NoopServerCallListener<>();
      } catch (StatusRuntimeException e) {
        Metadata trailers = e.getTrailers();
        call.close(e.getStatus(), trailers == null ? new Metadata() : trailers);
        return new NoopServerCallListener<>();
      }

      if (!peerIdentity.isPresent()) {
        // Send back a description along with the status code
        call.close(Status.UNAUTHENTICATED
            .withDescription("Unauthenticated (invalid or missing auth token)"), new Metadata());
        return new NoopServerCallListener<>();
      }
      return Contexts.interceptCall(Context.current().withValue(AuthConstants.PEER_IDENTITY_KEY, peerIdentity.get()),
          call, headers, next);
    }

    return next.startCall(call, headers);
  }

  private Optional<String> isValid(Metadata headers) {
    byte[] token = headers.get(AuthConstants.TOKEN_KEY);
    return authHandler.isValid(token);
  }

  private static class NoopServerCallListener<T> extends ServerCall.Listener<T> {
  }
}
