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

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import javax.net.ssl.SSLException;

import org.apache.arrow.flight.FlightProducer.StreamListener;
import org.apache.arrow.flight.auth.BasicClientAuthHandler;
import org.apache.arrow.flight.auth.ClientAuthHandler;
import org.apache.arrow.flight.auth.ClientAuthInterceptor;
import org.apache.arrow.flight.auth.ClientAuthWrapper;
import org.apache.arrow.flight.auth2.BasicAuthCredentialWriter;
import org.apache.arrow.flight.auth2.ClientBearerHeaderHandler;
import org.apache.arrow.flight.auth2.ClientHandshakeWrapper;
import org.apache.arrow.flight.auth2.ClientIncomingAuthHeaderMiddleware;
import org.apache.arrow.flight.grpc.ClientInterceptorAdapter;
import org.apache.arrow.flight.grpc.CredentialCallOption;
import org.apache.arrow.flight.grpc.StatusUtils;
import org.apache.arrow.flight.impl.Flight;
import org.apache.arrow.flight.impl.Flight.Empty;
import org.apache.arrow.flight.impl.FlightServiceGrpc;
import org.apache.arrow.flight.impl.FlightServiceGrpc.FlightServiceBlockingStub;
import org.apache.arrow.flight.impl.FlightServiceGrpc.FlightServiceStub;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.dictionary.DictionaryProvider.MapDictionaryProvider;

import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ClientInterceptors;
import io.grpc.ManagedChannel;
import io.grpc.MethodDescriptor;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.StreamObserver;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;

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
  private final MethodDescriptor<ArrowMessage, ArrowMessage> doExchangeDescriptor;
  private final List<FlightClientMiddleware.Factory> middleware;

  /**
   * Create a Flight client from an allocator and a gRPC channel.
   */
  FlightClient(BufferAllocator incomingAllocator, ManagedChannel channel,
      List<FlightClientMiddleware.Factory> middleware) {
    this.allocator = incomingAllocator.newChildAllocator("flight-client", 0, Long.MAX_VALUE);
    this.channel = channel;
    this.middleware = middleware;

    final ClientInterceptor[] interceptors;
    interceptors = new ClientInterceptor[]{authInterceptor, new ClientInterceptorAdapter(middleware)};

    // Create a channel with interceptors pre-applied for DoGet and DoPut
    Channel interceptedChannel = ClientInterceptors.intercept(channel, interceptors);

    blockingStub = FlightServiceGrpc.newBlockingStub(interceptedChannel);
    asyncStub = FlightServiceGrpc.newStub(interceptedChannel);
    doGetDescriptor = FlightBindingService.getDoGetDescriptor(allocator);
    doPutDescriptor = FlightBindingService.getDoPutDescriptor(allocator);
    doExchangeDescriptor = FlightBindingService.getDoExchangeDescriptor(allocator);
  }

  /**
   * Get a list of available flights.
   *
   * @param criteria Criteria for selecting flights
   * @param options RPC-layer hints for the call.
   * @return FlightInfo Iterable
   */
  public Iterable<FlightInfo> listFlights(Criteria criteria, CallOption... options) {
    final Iterator<Flight.FlightInfo> flights;
    try {
      flights = CallOptions.wrapStub(blockingStub, options)
          .listFlights(criteria.asCriteria());
    } catch (StatusRuntimeException sre) {
      throw StatusUtils.fromGrpcRuntimeException(sre);
    }
    return () -> StatusUtils.wrapIterator(flights, t -> {
      try {
        return new FlightInfo(t);
      } catch (URISyntaxException e) {
        // We don't expect this will happen for conforming Flight implementations. For instance, a Java server
        // itself wouldn't be able to construct an invalid Location.
        throw new RuntimeException(e);
      }
    });
  }

  /**
   * Lists actions available on the Flight service.
   *
   * @param options RPC-layer hints for the call.
   */
  public Iterable<ActionType> listActions(CallOption... options) {
    final Iterator<Flight.ActionType> actions;
    try {
      actions = CallOptions.wrapStub(blockingStub, options)
          .listActions(Empty.getDefaultInstance());
    } catch (StatusRuntimeException sre) {
      throw StatusUtils.fromGrpcRuntimeException(sre);
    }
    return () -> StatusUtils.wrapIterator(actions, ActionType::new);
  }

  /**
   * Performs an action on the Flight service.
   *
   * @param action The action to perform.
   * @param options RPC-layer hints for this call.
   * @return An iterator of results.
   */
  public Iterator<Result> doAction(Action action, CallOption... options) {
    return StatusUtils
        .wrapIterator(CallOptions.wrapStub(blockingStub, options).doAction(action.toProtocol()), Result::new);
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
   * Authenticates with a username and password.
   *
   * @param username the username.
   * @param password the password.
   * @return a CredentialCallOption containing a bearer token if the server emitted one, or
   *     empty if no bearer token was returned. This can be used in subsequent API calls.
   */
  public Optional<CredentialCallOption> authenticateBasicToken(String username, String password) {
    final ClientIncomingAuthHeaderMiddleware.Factory clientAuthMiddleware =
            new ClientIncomingAuthHeaderMiddleware.Factory(new ClientBearerHeaderHandler());
    middleware.add(clientAuthMiddleware);
    handshake(new CredentialCallOption(new BasicAuthCredentialWriter(username, password)));

    return Optional.ofNullable(clientAuthMiddleware.getCredentialCallOption());
  }

  /**
   * Executes the handshake against the Flight service.
   *
   * @param options RPC-layer hints for this call.
   */
  public void handshake(CallOption... options) {
    ClientHandshakeWrapper.doClientHandshake(CallOptions.wrapStub(asyncStub, options));
  }

  /**
   * Create or append a descriptor with another stream.
   *
   * @param descriptor FlightDescriptor the descriptor for the data
   * @param root VectorSchemaRoot the root containing data
   * @param metadataListener A handler for metadata messages from the server. This will be passed buffers that will be
   *     freed after {@link StreamListener#onNext(Object)} is called!
   * @param options RPC-layer hints for this call.
   * @return ClientStreamListener an interface to control uploading data
   */
  public ClientStreamListener startPut(FlightDescriptor descriptor, VectorSchemaRoot root,
      PutListener metadataListener, CallOption... options) {
    return startPut(descriptor, root, new MapDictionaryProvider(), metadataListener, options);
  }

  /**
   * Create or append a descriptor with another stream.
   * @param descriptor FlightDescriptor the descriptor for the data
   * @param root VectorSchemaRoot the root containing data
   * @param metadataListener A handler for metadata messages from the server.
   * @param options RPC-layer hints for this call.
   * @return ClientStreamListener an interface to control uploading data.
   *     {@link ClientStreamListener#start(VectorSchemaRoot, DictionaryProvider)} will already have been called.
   */
  public ClientStreamListener startPut(FlightDescriptor descriptor, VectorSchemaRoot root, DictionaryProvider provider,
      PutListener metadataListener, CallOption... options) {
    Preconditions.checkNotNull(root, "root must not be null");
    Preconditions.checkNotNull(provider, "provider must not be null");
    final ClientStreamListener writer = startPut(descriptor, metadataListener, options);
    writer.start(root, provider);
    return writer;
  }

  /**
   * Create or append a descriptor with another stream.
   * @param descriptor FlightDescriptor the descriptor for the data
   * @param metadataListener A handler for metadata messages from the server.
   * @param options RPC-layer hints for this call.
   * @return ClientStreamListener an interface to control uploading data.
   *     {@link ClientStreamListener#start(VectorSchemaRoot, DictionaryProvider)} will NOT already have been called.
   */
  public ClientStreamListener startPut(FlightDescriptor descriptor, PutListener metadataListener,
                                       CallOption... options) {
    Preconditions.checkNotNull(descriptor, "descriptor must not be null");
    Preconditions.checkNotNull(metadataListener, "metadataListener must not be null");

    try {
      final ClientCall<ArrowMessage, Flight.PutResult> call = asyncStubNewCall(doPutDescriptor, options);
      final SetStreamObserver resultObserver = new SetStreamObserver(allocator, metadataListener);
      ClientCallStreamObserver<ArrowMessage> observer = (ClientCallStreamObserver<ArrowMessage>)
          ClientCalls.asyncBidiStreamingCall(call, resultObserver);
      return new PutObserver(
          descriptor, observer, metadataListener::isCancelled, metadataListener::getResult);
    } catch (StatusRuntimeException sre) {
      throw StatusUtils.fromGrpcRuntimeException(sre);
    }
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
    } catch (StatusRuntimeException sre) {
      throw StatusUtils.fromGrpcRuntimeException(sre);
    }
  }

  /**
   * Start or get info on execution of a long-running query.
   *
   * @param descriptor The descriptor for the stream.
   * @param options RPC-layer hints for this call.
   * @return Metadata about execution.
   */
  public PollInfo pollInfo(FlightDescriptor descriptor, CallOption... options) {
    try {
      return new PollInfo(CallOptions.wrapStub(blockingStub, options).pollFlightInfo(descriptor.toProtocol()));
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    } catch (StatusRuntimeException sre) {
      throw StatusUtils.fromGrpcRuntimeException(sre);
    }
  }

  /**
   * Get schema for a stream.
   * @param descriptor The descriptor for the stream.
   * @param options RPC-layer hints for this call.
   */
  public SchemaResult getSchema(FlightDescriptor descriptor, CallOption... options) {
    try {
      return SchemaResult.fromProtocol(CallOptions.wrapStub(blockingStub, options)
          .getSchema(descriptor.toProtocol()));
    } catch (StatusRuntimeException sre) {
      throw StatusUtils.fromGrpcRuntimeException(sre);
    }
  }

  /**
   * Retrieve a stream from the server.
   * @param ticket The ticket granting access to the data stream.
   * @param options RPC-layer hints for this call.
   */
  public FlightStream getStream(Ticket ticket, CallOption... options) {
    final ClientCall<Flight.Ticket, ArrowMessage> call = asyncStubNewCall(doGetDescriptor, options);
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
            delegate.onError(StatusUtils.toGrpcException(t));
          }

          @Override
          public void onCompleted() {
            delegate.onCompleted();
          }

        };

    ClientCalls.asyncServerStreamingCall(call, ticket.toProtocol(), clientResponseObserver);
    return stream;
  }

  /**
   * Initiate a bidirectional data exchange with the server.
   *
   * @param descriptor A descriptor for the data stream.
   * @param options RPC call options.
   * @return A pair of a readable stream and a writable stream.
   */
  public ExchangeReaderWriter doExchange(FlightDescriptor descriptor, CallOption... options) {
    Preconditions.checkNotNull(descriptor, "descriptor must not be null");

    try {
      final ClientCall<ArrowMessage, ArrowMessage> call = asyncStubNewCall(doExchangeDescriptor, options);
      final FlightStream stream = new FlightStream(allocator, PENDING_REQUESTS, call::cancel, call::request);
      final ClientCallStreamObserver<ArrowMessage> observer = (ClientCallStreamObserver<ArrowMessage>)
              ClientCalls.asyncBidiStreamingCall(call, stream.asObserver());
      final ClientStreamListener writer = new PutObserver(
          descriptor, observer, stream.cancelled::isDone,
          () -> {
            try {
              stream.completed.get();
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              throw CallStatus.INTERNAL
                  .withDescription("Client error: interrupted while completing call")
                  .withCause(e)
                  .toRuntimeException();
            } catch (ExecutionException e) {
              throw CallStatus.INTERNAL
                  .withDescription("Client error: internal while completing call")
                  .withCause(e)
                  .toRuntimeException();
            }
          });
      // Send the descriptor to start.
      try (final ArrowMessage message = new ArrowMessage(descriptor.toProtocol())) {
        observer.onNext(message);
      } catch (Exception e) {
        throw CallStatus.INTERNAL
            .withCause(e)
            .withDescription("Could not write descriptor " + descriptor)
            .toRuntimeException();
      }
      return new ExchangeReaderWriter(stream, writer);
    } catch (StatusRuntimeException sre) {
      throw StatusUtils.fromGrpcRuntimeException(sre);
    }
  }

  /** A pair of a reader and a writer for a DoExchange call. */
  public static class ExchangeReaderWriter implements AutoCloseable {
    private final FlightStream reader;
    private final ClientStreamListener writer;

    ExchangeReaderWriter(FlightStream reader, ClientStreamListener writer) {
      this.reader = reader;
      this.writer = writer;
    }

    /** Get the reader for the call. */
    public FlightStream getReader() {
      return reader;
    }

    /** Get the writer for the call. */
    public ClientStreamListener getWriter() {
      return writer;
    }

    /**
     * Make sure stream is drained. You must call this to be notified of any errors that may have
     * happened after the exchange is complete. This should be called after `getWriter().completed()`
     * and instead of `getWriter().getResult()`.
     */
    public void getResult() {
      // After exchange is complete, make sure stream is drained to propagate errors through reader
      while (reader.next()) {
      }
    }

    /** Shut down the streams in this call. */
    @Override
    public void close() throws Exception {
      reader.close();
    }
  }

  /**
   * A stream observer for Flight.PutResult
   */
  private static class SetStreamObserver implements StreamObserver<Flight.PutResult> {
    private final BufferAllocator allocator;
    private final StreamListener<PutResult> listener;

    SetStreamObserver(BufferAllocator allocator, StreamListener<PutResult> listener) {
      super();
      this.allocator = allocator;
      this.listener = listener == null ? NoOpStreamListener.getInstance() : listener;
    }

    @Override
    public void onNext(Flight.PutResult value) {
      try (final PutResult message = PutResult.fromProtocol(allocator, value)) {
        listener.onNext(message);
      }
    }

    @Override
    public void onError(Throwable t) {
      listener.onError(StatusUtils.fromThrowable(t));
    }

    @Override
    public void onCompleted() {
      listener.onCompleted();
    }
  }

  /**
   * The implementation of a {@link ClientStreamListener} for writing data to a Flight server.
   */
  static class PutObserver extends OutboundStreamListenerImpl implements ClientStreamListener {
    private final BooleanSupplier isCancelled;
    private final Runnable getResult;

    /**
     * Create a new client stream listener.
     *
     * @param descriptor The descriptor for the stream.
     * @param observer The write-side gRPC StreamObserver.
     * @param isCancelled A flag to check if the call has been cancelled.
     * @param getResult A flag that blocks until the overall call completes.
     */
    PutObserver(FlightDescriptor descriptor, ClientCallStreamObserver<ArrowMessage> observer,
                BooleanSupplier isCancelled, Runnable getResult) {
      super(descriptor, observer);
      Preconditions.checkNotNull(descriptor, "descriptor must be provided");
      Preconditions.checkNotNull(isCancelled, "isCancelled must be provided");
      Preconditions.checkNotNull(getResult, "getResult must be provided");
      this.isCancelled = isCancelled;
      this.getResult = getResult;
      this.unloader = null;
    }

    @Override
    protected void waitUntilStreamReady() {
      // Check isCancelled as well to avoid inadvertently blocking forever
      // (so long as PutListener properly implements it)
      while (!responseObserver.isReady() && !isCancelled.getAsBoolean()) {
        /* busy wait */
      }
    }

    @Override
    public void getResult() {
      getResult.run();
    }
  }

  /**
   * Cancel execution of a distributed query.
   *
   * @param request The query to cancel.
   * @param options Call options.
   * @return The server response.
   */
  public CancelFlightInfoResult cancelFlightInfo(CancelFlightInfoRequest request, CallOption... options) {
    Action action = new Action(FlightConstants.CANCEL_FLIGHT_INFO.getType(), request.serialize().array());
    Iterator<Result> results = doAction(action, options);
    if (!results.hasNext()) {
      throw CallStatus.INTERNAL
          .withDescription("Server did not return a response")
          .toRuntimeException();
    }

    CancelFlightInfoResult result;
    try {
      result = CancelFlightInfoResult.deserialize(ByteBuffer.wrap(results.next().getBody()));
    } catch (IOException e) {
      throw CallStatus.INTERNAL
          .withDescription("Failed to parse server response: " + e)
          .withCause(e)
          .toRuntimeException();
    }
    results.forEachRemaining((ignored) -> {
    });
    return result;
  }

  /**
   * Request the server to extend the lifetime of a query result set.
   *
   * @param request The result set partition.
   * @param options Call options.
   * @return The new endpoint with an updated expiration time.
   */
  public FlightEndpoint renewFlightEndpoint(RenewFlightEndpointRequest request, CallOption... options) {
    Action action = new Action(FlightConstants.RENEW_FLIGHT_ENDPOINT.getType(), request.serialize().array());
    Iterator<Result> results = doAction(action, options);
    if (!results.hasNext()) {
      throw CallStatus.INTERNAL
          .withDescription("Server did not return a response")
          .toRuntimeException();
    }

    FlightEndpoint result;
    try {
      result = FlightEndpoint.deserialize(ByteBuffer.wrap(results.next().getBody()));
    } catch (IOException | URISyntaxException e) {
      throw CallStatus.INTERNAL
          .withDescription("Failed to parse server response: " + e)
          .withCause(e)
          .toRuntimeException();
    }
    results.forEachRemaining((ignored) -> {
    });
    return result;
  }

  /**
   * Set server session option(s) by name/value.
   *
   * Sessions are generally persisted via HTTP cookies.
   *
   * @param request The session options to set on the server.
   * @param options Call options.
   * @return The result containing per-value error statuses, if any.
   */
  public SetSessionOptionsResult setSessionOptions(SetSessionOptionsRequest request, CallOption... options) {
    Action action = new Action(FlightConstants.SET_SESSION_OPTIONS.getType(), request.serialize().array());
    Iterator<Result> results = doAction(action, options);
    if (!results.hasNext()) {
      throw CallStatus.INTERNAL
          .withDescription("Server did not return a response")
          .toRuntimeException();
    }

    SetSessionOptionsResult result;
    try {
      result = SetSessionOptionsResult.deserialize(ByteBuffer.wrap(results.next().getBody()));
    } catch (IOException e) {
      throw CallStatus.INTERNAL
          .withDescription("Failed to parse server response: " + e)
          .withCause(e)
          .toRuntimeException();
    }
    results.forEachRemaining((ignored) -> {
    });
    return result;
  }

  /**
   * Get the current server session options.
   *
   * The session is generally accessed via an HTTP cookie.
   *
   * @param request The (empty) GetSessionOptionsRequest.
   * @param options Call options.
   * @return The result containing the set of session options configured on the server.
   */
  public GetSessionOptionsResult getSessionOptions(GetSessionOptionsRequest request, CallOption... options) {
    Action action = new Action(FlightConstants.GET_SESSION_OPTIONS.getType(), request.serialize().array());
    Iterator<Result> results = doAction(action, options);
    if (!results.hasNext()) {
      throw CallStatus.INTERNAL
          .withDescription("Server did not return a response")
          .toRuntimeException();
    }

    GetSessionOptionsResult result;
    try {
      result = GetSessionOptionsResult.deserialize(ByteBuffer.wrap(results.next().getBody()));
    } catch (IOException e) {
      throw CallStatus.INTERNAL
          .withDescription("Failed to parse server response: " + e)
          .withCause(e)
          .toRuntimeException();
    }
    results.forEachRemaining((ignored) -> {
    });
    return result;
  }

  /**
   * Close/invalidate the current server session.
   *
   * The session is generally accessed via an HTTP cookie.
   *
   * @param request The (empty) CloseSessionRequest.
   * @param options Call options.
   * @return The result containing the status of the close operation.
   */
  public CloseSessionResult closeSession(CloseSessionRequest request, CallOption... options) {
    Action action = new Action(FlightConstants.CLOSE_SESSION.getType(), request.serialize().array());
    Iterator<Result> results = doAction(action, options);
    if (!results.hasNext()) {
      throw CallStatus.INTERNAL
          .withDescription("Server did not return a response")
          .toRuntimeException();
    }

    CloseSessionResult result;
    try {
      result = CloseSessionResult.deserialize(ByteBuffer.wrap(results.next().getBody()));
    } catch (IOException e) {
      throw CallStatus.INTERNAL
          .withDescription("Failed to parse server response: " + e)
          .withCause(e)
          .toRuntimeException();
    }
    results.forEachRemaining((ignored) -> {
    });
    return result;
  }

  /**
   * Interface for writers to an Arrow data stream.
   */
  public interface ClientStreamListener extends OutboundStreamListener {

    /**
     * Wait for the stream to finish on the server side. You must call this to be notified of any errors that may have
     * happened during the upload.
     */
    void getResult();
  }

  /**
   * A handler for server-sent application metadata messages during a Flight DoPut operation.
   *
   * <p>Generally, instead of implementing this yourself, you should use {@link AsyncPutListener} or {@link
   * SyncPutListener}.
   */
  public interface PutListener extends StreamListener<PutResult> {

    /**
     * Wait for the stream to finish on the server side. You must call this to be notified of any errors that may have
     * happened during the upload.
     */
    void getResult();

    /**
     * Called when a message from the server is received.
     *
     * @param val The application metadata. This buffer will be reclaimed once onNext returns; you must retain a
     *     reference to use it outside this method.
     */
    @Override
    void onNext(PutResult val);

    /**
     * Check if the call has been cancelled.
     *
     * <p>By default, this always returns false. Implementations should provide an appropriate implementation, as
     * otherwise, a DoPut operation may inadvertently block forever.
     */
    default boolean isCancelled() {
      return false;
    }
  }

  /**
   * Shut down this client.
   */
  @Override
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
    private String overrideHostname = null;
    private List<FlightClientMiddleware.Factory> middleware = new ArrayList<>();
    private boolean verifyServer = true;

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

    /** Override the hostname checked for TLS. Use with caution in production. */
    public Builder overrideHostname(final String hostname) {
      this.overrideHostname = hostname;
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

    public Builder intercept(FlightClientMiddleware.Factory factory) {
      middleware.add(factory);
      return this;
    }

    public Builder verifyServer(boolean verifyServer) {
      this.verifyServer = verifyServer;
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
                  Class.forName("io.netty.channel.epoll.EpollDomainSocketChannel")
                      .asSubclass(ServerChannel.class));
              final EventLoopGroup elg =
                  Class.forName("io.netty.channel.epoll.EpollEventLoopGroup").asSubclass(EventLoopGroup.class)
                  .getDeclaredConstructor().newInstance();
              builder.eventLoopGroup(elg);
            } catch (ClassNotFoundException e) {
              // BSD
              builder.channelType(
                  Class.forName("io.netty.channel.kqueue.KQueueDomainSocketChannel")
                      .asSubclass(ServerChannel.class));
              final EventLoopGroup elg = Class.forName("io.netty.channel.kqueue.KQueueEventLoopGroup")
                  .asSubclass(EventLoopGroup.class)
                  .getDeclaredConstructor().newInstance();
              builder.eventLoopGroup(elg);
            }
          } catch (ClassNotFoundException | InstantiationException | IllegalAccessException |
                   NoSuchMethodException | InvocationTargetException e) {
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

        final boolean hasTrustedCerts = this.trustedCertificates != null;
        final boolean hasKeyCertPair = this.clientCertificate != null && this.clientKey != null;
        if (!this.verifyServer && (hasTrustedCerts || hasKeyCertPair)) {
          throw new IllegalArgumentException("FlightClient has been configured to disable server verification, " +
              "but certificate options have been specified.");
        }

        final SslContextBuilder sslContextBuilder = GrpcSslContexts.forClient();

        if (!this.verifyServer) {
          sslContextBuilder.trustManager(InsecureTrustManagerFactory.INSTANCE);
        } else if (this.trustedCertificates != null || this.clientCertificate != null || this.clientKey != null) {
          if (this.trustedCertificates != null) {
            sslContextBuilder.trustManager(this.trustedCertificates);
          }
          if (this.clientCertificate != null && this.clientKey != null) {
            sslContextBuilder.keyManager(this.clientCertificate, this.clientKey);
          }
        }
        try {
          builder.sslContext(sslContextBuilder.build());
        } catch (SSLException e) {
          throw new RuntimeException(e);
        }

        if (this.overrideHostname != null) {
          builder.overrideAuthority(this.overrideHostname);
        }
      } else {
        builder.usePlaintext();
      }

      builder
          .maxTraceEvents(MAX_CHANNEL_TRACE_EVENTS)
          .maxInboundMessageSize(maxInboundMessageSize)
          .maxInboundMetadataSize(maxInboundMessageSize);
      return new FlightClient(allocator, builder.build(), middleware);
    }
  }

  /**
   * Helper method to create a call from the asyncStub, method descriptor, and list of calling options.
   */
  private <RequestT, ResponseT> ClientCall<RequestT, ResponseT> asyncStubNewCall(
          MethodDescriptor<RequestT, ResponseT> descriptor,
          CallOption... options) {
    FlightServiceStub wrappedStub = CallOptions.wrapStub(asyncStub, options);
    return wrappedStub.getChannel().newCall(descriptor, wrappedStub.getCallOptions());
  }
}
