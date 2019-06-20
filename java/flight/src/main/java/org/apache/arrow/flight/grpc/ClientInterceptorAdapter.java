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
import org.apache.arrow.flight.FlightClientMiddleware.Factory;
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

/**
 * An adapter between Flight client middleware and gRPC interceptors.
 */
public class ClientInterceptorAdapter implements ClientInterceptor {

  private final List<Factory> factories;

  public ClientInterceptorAdapter(List<Factory> factories) {
    this.factories = factories;
  }

  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> method,
      CallOptions callOptions, Channel next) {
    final List<FlightClientMiddleware> middleware = new ArrayList<>();
    final CallInfo info = new CallInfo(method.getFullMethodName());
    for (final Factory factory : factories) {
      middleware.add(factory.startCall(info));
    }

    return new SimpleForwardingClientCall<ReqT, RespT>(next.newCall(method, callOptions)) {
      @Override
      public void start(Listener<RespT> responseListener, Metadata headers) {
        final MetadataAdapter metadataAdapter = new MetadataAdapter(headers);
        middleware.forEach(m -> m.sendingHeaders(metadataAdapter));

        super.start(new SimpleForwardingClientCallListener<RespT>(responseListener) {
          boolean receivedHeaders = false;

          @Override
          public void onHeaders(Metadata headers) {
            receivedHeaders = true;
            final MetadataAdapter adapter = new MetadataAdapter(headers);
            middleware.forEach(m -> m.headersReceived(adapter));
            super.onHeaders(headers);
          }

          @Override
          public void onClose(Status status, Metadata trailers) {
            if (!receivedHeaders) {
              // gRPC doesn't always send response headers if the call errors or completes immediately
              final MetadataAdapter adapter = new MetadataAdapter(trailers);
              middleware.forEach(m -> m.headersReceived(adapter));
            }
            final CallStatus flightStatus = StatusUtils.fromGrpcStatus(status);
            middleware.forEach(m -> m.callCompleted(flightStatus));
            super.onClose(status, trailers);
          }
        }, headers);
      }

      @Override
      public void cancel(String message, Throwable cause) {
        final CallStatus flightStatus = new CallStatus(FlightStatusCode.CANCELLED, cause, message);
        middleware.forEach(m -> m.callCompleted(flightStatus));
        super.cancel(message, cause);
      }
    };
  }
}
