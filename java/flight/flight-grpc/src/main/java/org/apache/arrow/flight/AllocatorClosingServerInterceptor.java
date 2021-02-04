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

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;

/**
 * A server interceptor that closes {@link ArrowMessageMarshaller} instances.
 *
 * <p>This must be the FIRST interceptor run (which is the LAST one registered), as interceptors can choose to
 * abort a call chain at any point, and in that case, the application would leak a BufferAllocator.
 */
public class AllocatorClosingServerInterceptor implements ServerInterceptor {
  private static final Logger LOGGER = LoggerFactory.getLogger(AllocatorClosingServerInterceptor.class);
  private final BiConsumer<ServerCall<?, ?>, BufferAllocator> callback;
  private final AtomicInteger outstandingCalls;

  /**
   * Create a new interceptor.
   *
   * @param callback A callback for right before we close a BufferAllocator.
   *     Applications can use this to record metrics about the allocator.
   */
  public AllocatorClosingServerInterceptor(BiConsumer<ServerCall<?, ?>, BufferAllocator> callback) {
    this.callback = callback;
    this.outstandingCalls = new AtomicInteger(0);
  }

  /** Get the outstanding call count (useful for metrics reporting). */
  public int getOutstandingCalls() {
    return outstandingCalls.get();
  }

  /**
   * Wait for all tracked calls to finish.
   *
   * <p>gRPC does not wait for all onCancel/onComplete callbacks to finish on server shutdown. This method implements a
   * simple busy-wait so that you can ensure all those callbacks are finished (and hence all child allocators are
   * closed).
   *
   * <p>Should only be called after shutting down the gRPC server, before program exit.
   *
   * @throws InterruptedException if interrupted during waiting.
   * @throws CancellationException if the timeout expires and calls have not yet finished.
   */
  public void awaitTermination(long duration, TimeUnit unit) throws InterruptedException, CancellationException {
    long start = System.nanoTime();
    long end = start + unit.toNanos(duration);
    while (outstandingCalls.get() > 0 && System.nanoTime() < end) {
      Thread.sleep(100);
    }
    if (outstandingCalls.get() > 0) {
      throw new CancellationException("Timed out after " + duration + " " + unit +
          " with " + outstandingCalls.get() + " outstanding calls");
    }
  }

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
      ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
    Set<ArrowMessageMarshaller> arrowMessageMarshallers = new HashSet<>();
    if (call.getMethodDescriptor().getRequestMarshaller() instanceof ArrowMessageMarshaller) {
      arrowMessageMarshallers.add((ArrowMessageMarshaller) call.getMethodDescriptor().getRequestMarshaller());
    }
    if (call.getMethodDescriptor().getResponseMarshaller() instanceof ArrowMessageMarshaller) {
      arrowMessageMarshallers.add((ArrowMessageMarshaller) call.getMethodDescriptor().getResponseMarshaller());
    }
    if (arrowMessageMarshallers.isEmpty()) {
      // Bypass our logic
      return next.startCall(call, headers);
    }
    return new AllocatorClosingServerCallListener<>(next.startCall(call, headers), call, arrowMessageMarshallers);
  }

  private class AllocatorClosingServerCallListener<ReqT, RespT>
      extends ForwardingServerCallListener.SimpleForwardingServerCallListener<ReqT> {
    private final ServerCall<ReqT, RespT> call;
    private final Set<ArrowMessageMarshaller> allocators;

    public AllocatorClosingServerCallListener(ServerCall.Listener<ReqT> delegate,
                                              ServerCall<ReqT, RespT> call, Set<ArrowMessageMarshaller> allocators) {
      super(delegate);
      AllocatorClosingServerInterceptor.this.outstandingCalls.getAndIncrement();
      this.call = call;
      this.allocators = allocators;
    }

    private void cleanup(Runnable next) {
      Throwable t = null;
      try {
        allocators.forEach(marshaller -> callback.accept(call, marshaller.getAllocator()));
      } catch (RuntimeException e) {
        t = e;
      }
      try {
        if (t != null) {
          AutoCloseables.close(t, allocators);
        } else {
          AutoCloseables.close(allocators);
        }
      } catch (Exception e) {
        LOGGER.warn("Error closing per-call allocators", e);
      } finally {
        outstandingCalls.decrementAndGet();
        next.run();
      }
    }

    @Override
    public void onCancel() {
      cleanup(super::onCancel);
    }

    @Override
    public void onComplete() {
      cleanup(super::onComplete);
    }
  }
}
