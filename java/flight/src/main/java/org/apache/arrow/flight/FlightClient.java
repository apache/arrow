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

import static io.grpc.stub.ClientCalls.asyncClientStreamingCall;
import static io.grpc.stub.ClientCalls.asyncServerStreamingCall;

import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.net.ssl.SSLException;

import org.apache.arrow.flight.auth.BasicClientAuthHandler;
import org.apache.arrow.flight.auth.ClientAuthHandler;
import org.apache.arrow.flight.auth.ClientAuthInterceptor;
import org.apache.arrow.flight.auth.ClientAuthWrapper;
import org.apache.arrow.flight.impl.Flight;
import org.apache.arrow.flight.impl.Flight.Empty;
import org.apache.arrow.flight.impl.Flight.PutResult;
import org.apache.arrow.flight.impl.FlightServiceGrpc;
import org.apache.arrow.flight.impl.FlightServiceGrpc.FlightServiceBlockingStub;
import org.apache.arrow.flight.impl.FlightServiceGrpc.FlightServiceStub;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import io.grpc.ClientCall;
import io.grpc.ManagedChannel;
import io.grpc.MethodDescriptor;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.StreamObserver;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;

/**
 * Client for Flight services.
 */
public class FlightClient implements AutoCloseable {
  private static final int PENDING_REQUESTS = 5;
  /** The maximum number of trace events to keep on the gRPC Channel. This value disables channel tracing. */
  private static final int MAX_CHANNEL_TRACE_EVENTS = 0;
  private final BufferAllocator allocator;
  private final ManagedChannel channel;
  private final FlightServiceBlockingStub blockingStub;
  private final FlightServiceStub asyncStub;
  private final ClientAuthInterceptor authInterceptor = new ClientAuthInterceptor();
  private final MethodDescriptor<Flight.Ticket, ArrowMessage> doGetDescriptor;
  private final MethodDescriptor<ArrowMessage, Flight.PutResult> doPutDescriptor;

  private FlightClient(BufferAllocator incomingAllocator, ManagedChannel channel) {
    this.allocator = incomingAllocator.newChildAllocator("flight-client", 0, Long.MAX_VALUE);
    this.channel = channel;
    blockingStub = FlightServiceGrpc.newBlockingStub(channel).withInterceptors(authInterceptor);
    asyncStub = FlightServiceGrpc.newStub(channel).withInterceptors(authInterceptor);
    doGetDescriptor = FlightBindingService.getDoGetDescriptor(allocator);
    doPutDescriptor = FlightBindingService.getDoPutDescriptor(allocator);
  }

  /**
   * Get a list of available flights.
   *
   * @param criteria Criteria for selecting flights
   * @param options RPC-layer hints for the call.
   * @return FlightInfo Iterable
   */
  public Iterable<FlightInfo> listFlights(Criteria criteria, CallOption... options) {
    return ImmutableList.copyOf(CallOptions.wrapStub(blockingStub, options).listFlights(criteria.asCriteria()))
        .stream()
        .map(t -> {
          try {
            return new FlightInfo(t);
          } catch (URISyntaxException e) {
            // We don't expect this will happen for conforming Flight implementations. For instance, a Java server
            // itself wouldn't be able to construct an invalid Location.
            throw new RuntimeException(e);
          }
        })
        .collect(Collectors.toList());
  }

  /**
   * Lists actions available on the Flight service.
   *
   * @param options RPC-layer hints for the call.
   */
  public Iterable<ActionType> listActions(CallOption... options) {
    return ImmutableList.copyOf(CallOptions.wrapStub(blockingStub, options)
        .listActions(Empty.getDefaultInstance()))
        .stream()
        .map(ActionType::new)
        .collect(Collectors.toList());
  }

  /**
   * Performs an action on the Flight service.
   *
   * @param action The action to perform.
   * @param options RPC-layer hints for this call.
   * @return An iterator of results.
   */
  public Iterator<Result> doAction(Action action, CallOption... options) {
    return Iterators
        .transform(CallOptions.wrapStub(blockingStub, options).doAction(action.toProtocol()), Result::new);
  }

  /**
   * Authenticates with a username and password.
   */
  public void authenticateBasic(String username, String password) {
    BasicClientAuthHandler basicClient = new BasicClientAuthHandler(username, password);
    authenticate(basicClient);
  }

  /**
   * Authenticates against the Flight service.
   *
   * @param options RPC-layer hints for this call.
   * @param handler The auth mechanism to use.
   */
  public void authenticate(ClientAuthHandler handler, CallOption... options) {
    Preconditions.checkArgument(!authInterceptor.hasAuthHandler(), "Auth already completed.");
    ClientAuthWrapper.doClientAuth(handler, CallOptions.wrapStub(asyncStub, options));
    authInterceptor.setAuthHandler(handler);
  }

  /**
   * Create or append a descriptor with another stream.
   * @param descriptor FlightDescriptor
   * @param root VectorSchemaRoot
   * @param options RPC-layer hints for this call.
   * @return ClientStreamListener
   */
  public ClientStreamListener startPut(
      FlightDescriptor descriptor, VectorSchemaRoot root, CallOption... options) {
    Preconditions.checkNotNull(descriptor);
    Preconditions.checkNotNull(root);

    SetStreamObserver<PutResult> resultObserver = new SetStreamObserver<>();
    final io.grpc.CallOptions callOptions = CallOptions.wrapStub(asyncStub, options).getCallOptions();
    ClientCallStreamObserver<ArrowMessage> observer = (ClientCallStreamObserver<ArrowMessage>)
        asyncClientStreamingCall(
                authInterceptor.interceptCall(doPutDescriptor, callOptions, channel), resultObserver);
    // send the schema to start.
    ArrowMessage message = new ArrowMessage(descriptor.toProtocol(), root.getSchema());
    observer.onNext(message);
    return new PutObserver(new VectorUnloader(
        root, true /* include # of nulls in vectors */, true /* must align buffers to be C++-compatible */),
        observer, resultObserver.getFuture());
  }

  /**
   * Get info on a stream.
   * @param descriptor The descriptor for the stream.
   * @param options RPC-layer hints for this call.
   */
  public FlightInfo getInfo(FlightDescriptor descriptor, CallOption... options) {
    try {
      return new FlightInfo(CallOptions.wrapStub(blockingStub, options).getFlightInfo(descriptor.toProtocol()));
    } catch (URISyntaxException e) {
      // We don't expect this will happen for conforming Flight implementations. For instance, a Java server
      // itself wouldn't be able to construct an invalid Location.
      throw new RuntimeException(e);
    }
  }

  /**
   * Retrieve a stream from the server.
   * @param ticket The ticket granting access to the data stream.
   * @param options RPC-layer hints for this call.
   */
  public FlightStream getStream(Ticket ticket, CallOption... options) {
    final io.grpc.CallOptions callOptions = CallOptions.wrapStub(asyncStub, options).getCallOptions();
    ClientCall<Flight.Ticket, ArrowMessage> call =
            authInterceptor.interceptCall(doGetDescriptor, callOptions, channel);
    FlightStream stream = new FlightStream(
        allocator,
        PENDING_REQUESTS,
        (String message, Throwable cause) -> call.cancel(message, cause),
        (count) -> call.request(count));

    final StreamObserver<ArrowMessage> delegate = stream.asObserver();
    ClientResponseObserver<Flight.Ticket, ArrowMessage> clientResponseObserver =
        new ClientResponseObserver<Flight.Ticket, ArrowMessage>() {

          @Override
          public void beforeStart(ClientCallStreamObserver<org.apache.arrow.flight.impl.Flight.Ticket> requestStream) {
            requestStream.disableAutoInboundFlowControl();
          }

          @Override
          public void onNext(ArrowMessage value) {
            delegate.onNext(value);
          }

          @Override
          public void onError(Throwable t) {
            delegate.onError(t);
          }

          @Override
          public void onCompleted() {
            delegate.onCompleted();
          }

        };

    asyncServerStreamingCall(call, ticket.toProtocol(), clientResponseObserver);
    return stream;
  }

  private static class SetStreamObserver<T> implements StreamObserver<T> {
    private final SettableFuture<T> result = SettableFuture.create();
    private volatile T resultLocal;

    @Override
    public void onNext(T value) {
      resultLocal = value;
    }

    @Override
    public void onError(Throwable t) {
      result.setException(t);
    }

    @Override
    public void onCompleted() {
      result.set(Preconditions.checkNotNull(resultLocal));
    }

    public ListenableFuture<T> getFuture() {
      return result;
    }
  }

  private static class PutObserver implements ClientStreamListener {
    private final ClientCallStreamObserver<ArrowMessage> observer;
    private final VectorUnloader unloader;
    private final ListenableFuture<PutResult> futureResult;

    public PutObserver(VectorUnloader unloader, ClientCallStreamObserver<ArrowMessage> observer,
        ListenableFuture<PutResult> futureResult) {
      this.observer = observer;
      this.unloader = unloader;
      this.futureResult = futureResult;
    }

    @Override
    public void putNext() {
      ArrowRecordBatch batch = unloader.getRecordBatch();
      // Check the futureResult in case server sent an exception
      while (!observer.isReady() && !futureResult.isDone()) {
        /* busy wait */
      }
      observer.onNext(new ArrowMessage(batch));
    }

    @Override
    public void error(Throwable ex) {
      observer.onError(ex);
    }

    @Override
    public void completed() {
      observer.onCompleted();
    }

    @Override
    public PutResult getResult() {
      try {
        return futureResult.get();
      } catch (Exception ex) {
        throw Throwables.propagate(ex);
      }
    }
  }

  /**
   * Interface for subscribers to a stream returned by the server.
   */
  public interface ClientStreamListener {

    public void putNext();

    public void error(Throwable ex);

    public void completed();

    public PutResult getResult();

  }


  public void close() throws InterruptedException {
    channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    allocator.close();
  }

  /**
   * Create a builder for a Flight client.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Create a builder for a Flight client.
   * @param allocator The allocator to use for the client.
   * @param location The location to connect to.
   */
  public static Builder builder(BufferAllocator allocator, Location location) {
    return new Builder(allocator, location);
  }

  /**
   * A builder for Flight clients.
   */
  public static final class Builder {

    private BufferAllocator allocator;
    private Location location;
    private boolean forceTls = false;
    private int maxInboundMessageSize = FlightServer.MAX_GRPC_MESSAGE_SIZE;
    private InputStream trustedCertificates = null;
    private InputStream clientCertificate = null;
    private InputStream clientKey = null;

    private Builder() {
    }

    private Builder(BufferAllocator allocator, Location location) {
      this.allocator = Preconditions.checkNotNull(allocator);
      this.location = Preconditions.checkNotNull(location);
    }

    /**
     * Force the client to connect over TLS.
     */
    public Builder useTls() {
      this.forceTls = true;
      return this;
    }

    /** Set the maximum inbound message size. */
    public Builder maxInboundMessageSize(int maxSize) {
      Preconditions.checkArgument(maxSize > 0);
      this.maxInboundMessageSize = maxSize;
      return this;
    }

    /** Set the trusted TLS certificates. */
    public Builder trustedCertificates(final InputStream stream) {
      this.trustedCertificates = Preconditions.checkNotNull(stream);
      return this;
    }

    /** Set the trusted TLS certificates. */
    public Builder clientCertificate(final InputStream clientCertificate, final InputStream clientKey) {
      Preconditions.checkNotNull(clientKey);
      this.clientCertificate = Preconditions.checkNotNull(clientCertificate);
      this.clientKey = Preconditions.checkNotNull(clientKey);
      return this;
    }

    public Builder allocator(BufferAllocator allocator) {
      this.allocator = Preconditions.checkNotNull(allocator);
      return this;
    }

    public Builder location(Location location) {
      this.location = Preconditions.checkNotNull(location);
      return this;
    }

    /**
     * Create the client from this builder.
     */
    public FlightClient build() {
      final NettyChannelBuilder builder;

      switch (location.getUri().getScheme()) {
        case LocationSchemes.GRPC:
        case LocationSchemes.GRPC_INSECURE:
        case LocationSchemes.GRPC_TLS: {
          builder = NettyChannelBuilder.forAddress(location.toSocketAddress());
          break;
        }
        case LocationSchemes.GRPC_DOMAIN_SOCKET: {
          // The implementation is platform-specific, so we have to find the classes at runtime
          builder = NettyChannelBuilder.forAddress(location.toSocketAddress());
          try {
            try {
              // Linux
              builder.channelType(
                  (Class<? extends ServerChannel>) Class.forName("io.netty.channel.epoll.EpollDomainSocketChannel"));
              final EventLoopGroup elg = (EventLoopGroup) Class.forName("io.netty.channel.epoll.EpollEventLoopGroup")
                  .newInstance();
              builder.eventLoopGroup(elg);
            } catch (ClassNotFoundException e) {
              // BSD
              builder.channelType(
                  (Class<? extends ServerChannel>) Class.forName("io.netty.channel.kqueue.KQueueDomainSocketChannel"));
              final EventLoopGroup elg = (EventLoopGroup) Class.forName("io.netty.channel.kqueue.KQueueEventLoopGroup")
                  .newInstance();
              builder.eventLoopGroup(elg);
            }
          } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            throw new UnsupportedOperationException(
                "Could not find suitable Netty native transport implementation for domain socket address.");
          }
          break;
        }
        default:
          throw new IllegalArgumentException("Scheme is not supported: " + location.getUri().getScheme());
      }

      if (this.forceTls || LocationSchemes.GRPC_TLS.equals(location.getUri().getScheme())) {
        builder.useTransportSecurity();

        if (this.trustedCertificates != null || this.clientCertificate != null || this.clientKey != null) {
          final SslContextBuilder sslContextBuilder = GrpcSslContexts.forClient();
          if (this.trustedCertificates != null) {
            sslContextBuilder.trustManager(this.trustedCertificates);
          }
          if (this.clientCertificate != null && this.clientKey != null) {
            sslContextBuilder.keyManager(this.clientCertificate, this.clientKey);
          }
          try {
            builder.sslContext(sslContextBuilder.build());
          } catch (SSLException e) {
            throw new RuntimeException(e);
          }
        }
      } else {
        builder.usePlaintext();
      }

      builder
          .maxTraceEvents(MAX_CHANNEL_TRACE_EVENTS)
          .maxInboundMessageSize(maxInboundMessageSize);
      return new FlightClient(allocator, builder.build());
    }
  }
}
