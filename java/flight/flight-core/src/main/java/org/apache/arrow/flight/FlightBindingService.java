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

package org.apache.arrow.flight;

import java.util.Set;
import java.util.concurrent.ExecutorService;

import org.apache.arrow.flight.auth.ServerAuthHandler;
import org.apache.arrow.flight.impl.Flight;
import org.apache.arrow.flight.impl.Flight.PutResult;
import org.apache.arrow.flight.impl.FlightServiceGrpc;
import org.apache.arrow.memory.BufferAllocator;

import com.google.common.collect.ImmutableSet;

import io.grpc.BindableService;
import io.grpc.MethodDescriptor;
import io.grpc.MethodDescriptor.MethodType;
import io.grpc.ServerMethodDefinition;
import io.grpc.ServerServiceDefinition;
import io.grpc.ServiceDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ServerCalls;
import io.grpc.stub.StreamObserver;

/**
 * Extends the basic flight service to override some methods for more efficient implementations.
 */
class FlightBindingService implements BindableService {

  private static final String DO_GET = MethodDescriptor.generateFullMethodName(FlightConstants.SERVICE, "DoGet");
  private static final String DO_PUT = MethodDescriptor.generateFullMethodName(FlightConstants.SERVICE, "DoPut");
  private static final String DO_EXCHANGE = MethodDescriptor.generateFullMethodName(
      FlightConstants.SERVICE, "DoExchange");
  private static final Set<String> OVERRIDE_METHODS = ImmutableSet.of(DO_GET, DO_PUT, DO_EXCHANGE);

  private final FlightService delegate;
  private final BufferAllocator allocator;

  public FlightBindingService(BufferAllocator allocator, FlightProducer producer,
      ServerAuthHandler authHandler, ExecutorService executor) {
    this.allocator = allocator;
    this.delegate = new FlightService(allocator, producer, authHandler, executor);
  }

  public static MethodDescriptor<Flight.Ticket, ArrowMessage> getDoGetDescriptor(BufferAllocator allocator) {
    return MethodDescriptor.<Flight.Ticket, ArrowMessage>newBuilder()
        .setType(io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
        .setFullMethodName(DO_GET)
        .setSampledToLocalTracing(false)
        .setRequestMarshaller(ProtoUtils.marshaller(Flight.Ticket.getDefaultInstance()))
        .setResponseMarshaller(ArrowMessage.createMarshaller(allocator))
        .setSchemaDescriptor(FlightServiceGrpc.getDoGetMethod().getSchemaDescriptor())
        .build();
  }

  public static MethodDescriptor<ArrowMessage, Flight.PutResult> getDoPutDescriptor(BufferAllocator allocator) {
    return MethodDescriptor.<ArrowMessage, Flight.PutResult>newBuilder()
        .setType(MethodType.BIDI_STREAMING)
        .setFullMethodName(DO_PUT)
        .setSampledToLocalTracing(false)
        .setRequestMarshaller(ArrowMessage.createMarshaller(allocator))
        .setResponseMarshaller(ProtoUtils.marshaller(Flight.PutResult.getDefaultInstance()))
        .setSchemaDescriptor(FlightServiceGrpc.getDoPutMethod().getSchemaDescriptor())
        .build();
  }

  public static MethodDescriptor<ArrowMessage, ArrowMessage> getDoExchangeDescriptor(BufferAllocator allocator) {
    return MethodDescriptor.<ArrowMessage, ArrowMessage>newBuilder()
            .setType(MethodType.BIDI_STREAMING)
            .setFullMethodName(DO_EXCHANGE)
            .setSampledToLocalTracing(false)
            .setRequestMarshaller(ArrowMessage.createMarshaller(allocator))
            .setResponseMarshaller(ArrowMessage.createMarshaller(allocator))
            .setSchemaDescriptor(FlightServiceGrpc.getDoExchangeMethod().getSchemaDescriptor())
            .build();
  }

  @Override
  public ServerServiceDefinition bindService() {
    final ServerServiceDefinition baseDefinition = delegate.bindService();

    final MethodDescriptor<Flight.Ticket, ArrowMessage> doGetDescriptor = getDoGetDescriptor(allocator);
    final MethodDescriptor<ArrowMessage, Flight.PutResult> doPutDescriptor = getDoPutDescriptor(allocator);
    final MethodDescriptor<ArrowMessage, ArrowMessage> doExchangeDescriptor = getDoExchangeDescriptor(allocator);

    // Make sure we preserve SchemaDescriptor fields on methods so that gRPC reflection still works.
    final ServiceDescriptor.Builder serviceDescriptorBuilder = ServiceDescriptor.newBuilder(FlightConstants.SERVICE)
        .setSchemaDescriptor(baseDefinition.getServiceDescriptor().getSchemaDescriptor());
    serviceDescriptorBuilder.addMethod(doGetDescriptor);
    serviceDescriptorBuilder.addMethod(doPutDescriptor);
    serviceDescriptorBuilder.addMethod(doExchangeDescriptor);
    for (MethodDescriptor<?, ?> definition : baseDefinition.getServiceDescriptor().getMethods()) {
      if (OVERRIDE_METHODS.contains(definition.getFullMethodName())) {
        continue;
      }

      serviceDescriptorBuilder.addMethod(definition);
    }

    final ServiceDescriptor serviceDescriptor = serviceDescriptorBuilder.build();
    ServerServiceDefinition.Builder serviceBuilder = ServerServiceDefinition.builder(serviceDescriptor);
    serviceBuilder.addMethod(doGetDescriptor, ServerCalls.asyncServerStreamingCall(new DoGetMethod(delegate)));
    serviceBuilder.addMethod(doPutDescriptor, ServerCalls.asyncBidiStreamingCall(new DoPutMethod(delegate)));
    serviceBuilder.addMethod(doExchangeDescriptor, ServerCalls.asyncBidiStreamingCall(new DoExchangeMethod(delegate)));

    // copy over not-overridden methods.
    for (ServerMethodDefinition<?, ?> definition : baseDefinition.getMethods()) {
      if (OVERRIDE_METHODS.contains(definition.getMethodDescriptor().getFullMethodName())) {
        continue;
      }

      serviceBuilder.addMethod(definition);
    }

    return serviceBuilder.build();
  }

  private static class DoGetMethod implements ServerCalls.ServerStreamingMethod<Flight.Ticket, ArrowMessage> {

    private final FlightService delegate;

    public DoGetMethod(FlightService delegate) {
      this.delegate = delegate;
    }

    @Override
    public void invoke(Flight.Ticket request, StreamObserver<ArrowMessage> responseObserver) {
      delegate.doGetCustom(request, responseObserver);
    }
  }

  private static class DoPutMethod implements ServerCalls.BidiStreamingMethod<ArrowMessage, PutResult> {
    private final FlightService delegate;

    public DoPutMethod(FlightService delegate) {
      this.delegate = delegate;
    }

    @Override
    public StreamObserver<ArrowMessage> invoke(StreamObserver<PutResult> responseObserver) {
      return delegate.doPutCustom(responseObserver);
    }
  }

  private static class DoExchangeMethod implements ServerCalls.BidiStreamingMethod<ArrowMessage, ArrowMessage> {
    private final FlightService delegate;

    public DoExchangeMethod(FlightService delegate) {
      this.delegate = delegate;
    }

    @Override
    public StreamObserver<ArrowMessage> invoke(StreamObserver<ArrowMessage> responseObserver) {
      return delegate.doExchangeCustom(responseObserver);
    }
  }

}
