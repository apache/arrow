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

import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.arrow.flight.auth.ServerAuthHandler;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.VisibleForTesting;

import io.grpc.BindableService;
import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.MethodDescriptor;

/**
 * Exposes Flight GRPC service & client.
 */
public class FlightGrpcUtils {
  /**
   * Proxy class for ManagedChannel that makes closure a no-op.
   */
  @VisibleForTesting
  static class NonClosingProxyManagedChannel extends ManagedChannel {
    private final ManagedChannel channel;
    private boolean isShutdown;

    NonClosingProxyManagedChannel(ManagedChannel channel) {
      this.channel = channel;
      this.isShutdown = channel.isShutdown();
    }

    @Override
    public ManagedChannel shutdown() {
      isShutdown = true;
      return this;
    }

    @Override
    public boolean isShutdown() {
      if (this.channel.isShutdown()) {
        // If the underlying channel is shut down, ensure we're updated to match.
        shutdown();
      }
      return isShutdown;
    }

    @Override
    public boolean isTerminated() {
      return this.isShutdown();
    }

    @Override
    public ManagedChannel shutdownNow() {
      return shutdown();
    }

    @Override
    public boolean awaitTermination(long l, TimeUnit timeUnit) {
      // Don't actually await termination, since it'll be a no-op, so simply return whether or not
      // the channel has been shut down already.
      return this.isShutdown();
    }

    @Override
    public <RequestT, ResponseT> ClientCall<RequestT, ResponseT> newCall(
        MethodDescriptor<RequestT, ResponseT> methodDescriptor, CallOptions callOptions) {
      if (this.isShutdown()) {
        throw new IllegalStateException("Channel has been shut down.");
      }

      return this.channel.newCall(methodDescriptor, callOptions);
    }

    @Override
    public String authority() {
      return this.channel.authority();
    }

    @Override
    public ConnectivityState getState(boolean requestConnection) {
      if (this.isShutdown()) {
        return ConnectivityState.SHUTDOWN;
      }

      return this.channel.getState(requestConnection);
    }

    @Override
    public void notifyWhenStateChanged(ConnectivityState source, Runnable callback) {
      // The proxy has no insight into the underlying channel state changes, so we'll have to leak the abstraction
      // a bit here and simply pass to the underlying channel, even though it will never transition to shutdown via
      // the proxy. This should be fine, since it's mainly targeted at the FlightClient and there's no getter for
      // the channel.
      this.channel.notifyWhenStateChanged(source, callback);
    }

    @Override
    public void resetConnectBackoff() {
      this.channel.resetConnectBackoff();
    }

    @Override
    public void enterIdle() {
      this.channel.enterIdle();
    }
  }

  private FlightGrpcUtils() {
  }

  /**
   * Creates a Flight service.
   * @param allocator Memory allocator
   * @param producer Specifies the service api
   * @param authHandler Authentication handler
   * @param executor Executor service
   * @return FlightBindingService
   */
  public static BindableService createFlightService(BufferAllocator allocator, FlightProducer producer,
                                                    ServerAuthHandler authHandler, ExecutorService executor) {
    return new FlightBindingService(allocator, producer, authHandler, executor);
  }

  /**
   * Creates a Flight client.
   * @param incomingAllocator  Memory allocator
   * @param channel provides a connection to a gRPC server.
   */
  public static FlightClient createFlightClient(BufferAllocator incomingAllocator, ManagedChannel channel) {
    return new FlightClient(incomingAllocator, channel, Collections.emptyList());
  }

  /**
   * Creates a Flight client.
   * @param incomingAllocator  Memory allocator
   * @param channel provides a connection to a gRPC server. Will not be closed on closure of the returned FlightClient.
   */
  public static FlightClient createFlightClientWithSharedChannel(
      BufferAllocator incomingAllocator, ManagedChannel channel) {
    return new FlightClient(incomingAllocator, new NonClosingProxyManagedChannel(channel), Collections.emptyList());
  }
}
