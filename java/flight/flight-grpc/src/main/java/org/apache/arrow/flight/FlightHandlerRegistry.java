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

import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.arrow.flight.impl.Flight;
import org.apache.arrow.memory.BufferAllocator;

import io.grpc.Context;
import io.grpc.HandlerRegistry;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerMethodDefinition;
import io.grpc.ServerServiceDefinition;
import io.grpc.Status;

/**
 * A gRPC HandlerRegistry that creates a new Arrow allocator for each call to a Flight "data" method
 * (DoGet, DoPut, or DoExchange).
 */
public final class FlightHandlerRegistry extends HandlerRegistry {
  private final BufferAllocator allocator;
  private final ServerServiceDefinition delegate;
  private final ServerMethodDefinition<Flight.Ticket, ArrowMessage> doGetMethod;
  private final ServerMethodDefinition<ArrowMessage, Flight.PutResult> doPutMethod;
  private final ServerMethodDefinition<ArrowMessage, ArrowMessage> doExchangeMethod;
  private final AtomicInteger counter;

  @SuppressWarnings("unchecked")
  FlightHandlerRegistry(BufferAllocator allocator, ServerServiceDefinition delegate) {
    this.allocator = allocator;
    this.delegate = delegate;
    // Unchecked cast
    this.doGetMethod = (ServerMethodDefinition<Flight.Ticket, ArrowMessage>)
        Objects.requireNonNull(delegate.getMethod(FlightBindingService.DO_GET));
    this.doPutMethod = (ServerMethodDefinition<ArrowMessage, Flight.PutResult>)
        Objects.requireNonNull(delegate.getMethod(FlightBindingService.DO_PUT));
    this.doExchangeMethod = (ServerMethodDefinition<ArrowMessage, ArrowMessage>)
        Objects.requireNonNull(delegate.getMethod(FlightBindingService.DO_EXCHANGE));
    this.counter = new AtomicInteger(0);
  }

  @Override
  public ServerMethodDefinition<?, ?> lookupMethod(String methodName, String authority) {
    if (FlightBindingService.DO_GET.equals(methodName)) {
      final BufferAllocator childAllocator = newChildAllocator("DoGet");
      final MethodDescriptor.Marshaller<ArrowMessage> marshaller = new ArrowMessageMarshaller(childAllocator);
      final MethodDescriptor<Flight.Ticket, ArrowMessage> method =
          doGetMethod.getMethodDescriptor().toBuilder().setResponseMarshaller(marshaller).build();
      final ServerCallHandler<Flight.Ticket, ArrowMessage> handler =
          new AllocatorInjectingServerCallHandler<>(doGetMethod.getServerCallHandler(), childAllocator);
      return ServerMethodDefinition.create(method, handler);
    } else if (FlightBindingService.DO_PUT.equals(methodName)) {
      final BufferAllocator childAllocator = newChildAllocator("DoPut");
      final MethodDescriptor.Marshaller<ArrowMessage> marshaller = new ArrowMessageMarshaller(childAllocator);
      final MethodDescriptor<ArrowMessage, Flight.PutResult> method =
          doPutMethod.getMethodDescriptor().toBuilder().setRequestMarshaller(marshaller).build();
      final ServerCallHandler<ArrowMessage, Flight.PutResult> handler =
          new AllocatorInjectingServerCallHandler<>(doPutMethod.getServerCallHandler(), childAllocator);
      return ServerMethodDefinition.create(method, handler);
    } else if (FlightBindingService.DO_EXCHANGE.equals(methodName)) {
      final BufferAllocator childAllocator = newChildAllocator("DoExchange");
      final MethodDescriptor.Marshaller<ArrowMessage> marshaller = new ArrowMessageMarshaller(childAllocator);
      final MethodDescriptor<ArrowMessage, ArrowMessage> method = doExchangeMethod.getMethodDescriptor()
          .toBuilder()
          .setRequestMarshaller(marshaller)
          .setResponseMarshaller(marshaller)
          .build();
      final ServerCallHandler<ArrowMessage, ArrowMessage> handler =
          new AllocatorInjectingServerCallHandler<>(doExchangeMethod.getServerCallHandler(), childAllocator);
      return ServerMethodDefinition.create(method, handler);
    }
    return delegate.getMethod(methodName);
  }

  /**
   * Create a new child allocator for a call to the given method.
   *
   * @param methodName The Flight method being called.
   */
  private BufferAllocator newChildAllocator(final String methodName) {
    final String allocatorName = allocator.getName() + "-" + methodName + "-" + counter.getAndIncrement();
    return allocator.newChildAllocator(allocatorName, 0, allocator.getLimit());
  }

  /**
   * A ServerCallHandler that injects the Arrow allocator into the gRPC context.
   *
   * @param <ReqT> The request type.
   * @param <RespT> The response type.
   */
  static final class AllocatorInjectingServerCallHandler<ReqT, RespT> implements ServerCallHandler<ReqT, RespT> {
    private final ServerCallHandler<ReqT, RespT> delegate;
    private final BufferAllocator allocator;

    AllocatorInjectingServerCallHandler(ServerCallHandler<ReqT, RespT> delegate, BufferAllocator allocator) {
      this.delegate = delegate;
      this.allocator = allocator;
    }

    @Override
    public ServerCall.Listener<ReqT> startCall(ServerCall<ReqT, RespT> call, Metadata headers) {
      final ServerCall.Listener<ReqT> delegateListener;
      try {
        delegateListener = Context.current()
            .withValue(FlightGrpcUtils.PER_CALL_ALLOCATOR, allocator)
            .call(() -> delegate.startCall(call, headers));
      } catch (Exception e) {
        allocator.close();
        call.close(Status.INTERNAL.withCause(e).withDescription("Internal error: " + e), new Metadata());
        return new ServerCall.Listener<ReqT>() {};
      }
      return new AllocatorInjectingServerCallListener<>(delegateListener, allocator);
    }
  }

  /**
   * A ServerCallListener that injects the Arrow allocator into the gRPC context.
   * @param <ReqT> The request type.
   */
  static final class AllocatorInjectingServerCallListener<ReqT> extends ServerCall.Listener<ReqT> {
    private final ServerCall.Listener<ReqT> delegate;
    private final BufferAllocator allocator;

    AllocatorInjectingServerCallListener(ServerCall.Listener<ReqT> delegate, BufferAllocator allocator) {
      this.delegate = delegate;
      this.allocator = allocator;
    }

    @Override
    public void onMessage(ReqT message) {
      Context.current().withValue(FlightGrpcUtils.PER_CALL_ALLOCATOR, allocator).run(() -> delegate.onMessage(message));
    }

    @Override
    public void onHalfClose() {
      Context.current().withValue(FlightGrpcUtils.PER_CALL_ALLOCATOR, allocator).run(delegate::onHalfClose);
    }

    @Override
    public void onCancel() {
      Context.current().withValue(FlightGrpcUtils.PER_CALL_ALLOCATOR, allocator).run(delegate::onCancel);
    }

    @Override
    public void onComplete() {
      Context.current().withValue(FlightGrpcUtils.PER_CALL_ALLOCATOR, allocator).run(delegate::onComplete);
    }

    @Override
    public void onReady() {
      Context.current().withValue(FlightGrpcUtils.PER_CALL_ALLOCATOR, allocator).run(delegate::onReady);
    }
  }
}
