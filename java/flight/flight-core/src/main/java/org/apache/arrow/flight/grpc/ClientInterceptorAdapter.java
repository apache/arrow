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
import java.util.List;

import org.apache.arrow.flight.CallInfo;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightClientMiddleware;
import org.apache.arrow.flight.FlightMethod;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall.SimpleForwardingClientCall;
import io.grpc.ForwardingClientCallListener.SimpleForwardingClientCallListener;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

/**
 * An adapter between Flight client middleware and gRPC interceptors.
 *
 * <p>This is implemented as a single gRPC interceptor that runs all Flight client middleware sequentially.
 */
public class ClientInterceptorAdapter implements ClientInterceptor {

  private final List<FlightClientMiddleware.Factory> factories;

  public ClientInterceptorAdapter(List<FlightClientMiddleware.Factory> factories) {
    this.factories = factories;
  }

  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> method,
      CallOptions callOptions, Channel next) {
    final List<FlightClientMiddleware> middleware = new ArrayList<>();
    final CallInfo info = new CallInfo(FlightMethod.fromProtocol(method.getFullMethodName()));

    try {
      for (final FlightClientMiddleware.Factory factory : factories) {
        middleware.add(factory.onCallStarted(info));
      }
    } catch (FlightRuntimeException e) {
      // Explicitly propagate
      throw e;
    } catch (StatusRuntimeException e) {
      throw StatusUtils.fromGrpcRuntimeException(e);
    } catch (RuntimeException e) {
      throw StatusUtils.fromThrowable(e);
    }
    return new FlightClientCall<>(next.newCall(method, callOptions), middleware);
  }

  /**
   * The ClientCallListener which hooks into the gRPC request cycle and actually runs middleware at certain points.
   */
  private static class FlightClientCallListener<RespT> extends SimpleForwardingClientCallListener<RespT> {

    private final List<FlightClientMiddleware> middleware;
    boolean receivedHeaders;

    public FlightClientCallListener(ClientCall.Listener<RespT> responseListener,
        List<FlightClientMiddleware> middleware) {
      super(responseListener);
      this.middleware = middleware;
      receivedHeaders = false;
    }

    @Override
    public void onHeaders(Metadata headers) {
      receivedHeaders = true;
      final MetadataAdapter adapter = new MetadataAdapter(headers);
      try {
        middleware.forEach(m -> m.onHeadersReceived(adapter));
      } finally {
        // Make sure to always call the gRPC callback to avoid interrupting the gRPC request cycle
        super.onHeaders(headers);
      }
    }

    @Override
    public void onClose(Status status, Metadata trailers) {
      try {
        if (!receivedHeaders) {
          // gRPC doesn't always send response headers if the call errors or completes immediately, but instead
          // consolidates them with the trailers. If we never got headers, assume this happened and run the header
          // callback with the trailers.
          final MetadataAdapter adapter = new MetadataAdapter(trailers);
          middleware.forEach(m -> m.onHeadersReceived(adapter));
        }
        final CallStatus flightStatus = StatusUtils.fromGrpcStatusAndTrailers(status, trailers);
        middleware.forEach(m -> m.onCallCompleted(flightStatus));
      } finally {
        // Make sure to always call the gRPC callback to avoid interrupting the gRPC request cycle
        super.onClose(status, trailers);
      }
    }
  }

  /**
   * The gRPC ClientCall which hooks into the gRPC request cycle and injects our ClientCallListener.
   */
  private static class FlightClientCall<ReqT, RespT> extends SimpleForwardingClientCall<ReqT, RespT> {

    private final List<FlightClientMiddleware> middleware;

    public FlightClientCall(ClientCall<ReqT, RespT> clientCall, List<FlightClientMiddleware> middleware) {
      super(clientCall);
      this.middleware = middleware;
    }

    @Override
    public void start(Listener<RespT> responseListener, Metadata headers) {
      final MetadataAdapter metadataAdapter = new MetadataAdapter(headers);
      middleware.forEach(m -> m.onBeforeSendingHeaders(metadataAdapter));

      super.start(new FlightClientCallListener<>(responseListener, middleware), headers);
    }

    @Override
    public void cancel(String message, Throwable cause) {
      final CallStatus flightStatus = new CallStatus(FlightStatusCode.CANCELLED, cause, message, null);
      middleware.forEach(m -> m.onCallCompleted(flightStatus));
      super.cancel(message, cause);
    }
  }
}
