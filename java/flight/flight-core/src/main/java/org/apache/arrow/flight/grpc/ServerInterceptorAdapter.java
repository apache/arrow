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

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.arrow.flight.CallInfo;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightConstants;
import org.apache.arrow.flight.FlightMethod;
import org.apache.arrow.flight.FlightProducer.CallContext;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightServerMiddleware;
import org.apache.arrow.flight.FlightServerMiddleware.Factory;
import org.apache.arrow.flight.FlightServerMiddleware.Key;

import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.ForwardingServerCall.SimpleForwardingServerCall;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCall.Listener;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;

/**
 * An adapter between Flight middleware and a gRPC interceptor.
 *
 * <p>This is implemented as a single gRPC interceptor that runs all Flight server middleware sequentially. Flight
 * middleware instances are stored in the gRPC Context so their state is accessible later.
 */
public class ServerInterceptorAdapter implements ServerInterceptor {

  /**
   * A combination of a middleware Key and factory.
   *
   * @param <T> The middleware type.
   */
  public static class KeyFactory<T extends FlightServerMiddleware> {

    private final FlightServerMiddleware.Key<T> key;
    private final FlightServerMiddleware.Factory<T> factory;

    public KeyFactory(Key<T> key, Factory<T> factory) {
      this.key = key;
      this.factory = factory;
    }
  }

  /**
   * The {@link Context.Key} that stores the Flight middleware active for a particular call.
   *
   * <p>Applications should not use this directly. Instead, see {@link CallContext#getMiddleware(Key)}.
   */
  public static final Context.Key<Map<FlightServerMiddleware.Key<?>, FlightServerMiddleware>> SERVER_MIDDLEWARE_KEY =
      Context.key("arrow.flight.server_middleware");
  private final List<KeyFactory<?>> factories;

  public ServerInterceptorAdapter(List<KeyFactory<?>> factories) {
    this.factories = factories;
  }

  @Override
  public <ReqT, RespT> Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers,
      ServerCallHandler<ReqT, RespT> next) {
    if (!FlightConstants.SERVICE.equals(call.getMethodDescriptor().getServiceName())) {
      return Contexts.interceptCall(Context.current(), call, headers, next);
    }
    
    final CallInfo info = new CallInfo(FlightMethod.fromProtocol(call.getMethodDescriptor().getFullMethodName()));
    final List<FlightServerMiddleware> middleware = new ArrayList<>();
    // Use LinkedHashMap to preserve insertion order
    final Map<FlightServerMiddleware.Key<?>, FlightServerMiddleware> middlewareMap = new LinkedHashMap<>();
    final MetadataAdapter headerAdapter = new MetadataAdapter(headers);
    final RequestContextAdapter requestContextAdapter = new RequestContextAdapter();
    for (final KeyFactory<?> factory : factories) {
      final FlightServerMiddleware m;
      try {
        m = factory.factory.onCallStarted(info, headerAdapter, requestContextAdapter);
      } catch (FlightRuntimeException e) {
        // Cancel call
        call.close(StatusUtils.toGrpcStatus(e.status()), new Metadata());
        return new Listener<ReqT>() {};
      }
      middleware.add(m);
      middlewareMap.put(factory.key, m);
    }

    // Inject the middleware into the context so RPC method implementations can communicate with middleware instances
    final Context contextWithMiddlewareAndRequestsOptions = Context.current()
        .withValue(SERVER_MIDDLEWARE_KEY, Collections.unmodifiableMap(middlewareMap))
        .withValue(RequestContextAdapter.REQUEST_CONTEXT_KEY, requestContextAdapter);

    final SimpleForwardingServerCall<ReqT, RespT> forwardingServerCall = new SimpleForwardingServerCall<ReqT, RespT>(
        call) {
      boolean sentHeaders = false;

      @Override
      public void sendHeaders(Metadata headers) {
        sentHeaders = true;
        try {
          final MetadataAdapter headerAdapter = new MetadataAdapter(headers);
          middleware.forEach(m -> m.onBeforeSendingHeaders(headerAdapter));
        } finally {
          // Make sure to always call the gRPC callback to avoid interrupting the gRPC request cycle
          super.sendHeaders(headers);
        }
      }

      @Override
      public void close(Status status, Metadata trailers) {
        try {
          if (!sentHeaders) {
            // gRPC doesn't always send response headers if the call errors or completes immediately
            final MetadataAdapter headerAdapter = new MetadataAdapter(trailers);
            middleware.forEach(m -> m.onBeforeSendingHeaders(headerAdapter));
          }
        } finally {
          // Make sure to always call the gRPC callback to avoid interrupting the gRPC request cycle
          super.close(status, trailers);
        }

        final CallStatus flightStatus = StatusUtils.fromGrpcStatus(status);
        middleware.forEach(m -> m.onCallCompleted(flightStatus));
      }
    };
    return Contexts.interceptCall(contextWithMiddlewareAndRequestsOptions, forwardingServerCall, headers, next);

  }
}
