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
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

import org.apache.arrow.flight.FlightProducer.ServerStreamListener;
import org.apache.arrow.flight.FlightServerMiddleware.Key;
import org.apache.arrow.flight.auth.AuthConstants;
import org.apache.arrow.flight.auth.ServerAuthHandler;
import org.apache.arrow.flight.auth.ServerAuthWrapper;
import org.apache.arrow.flight.grpc.ContextPropagatingExecutorService;
import org.apache.arrow.flight.grpc.ServerInterceptorAdapter;
import org.apache.arrow.flight.grpc.StatusUtils;
import org.apache.arrow.flight.impl.Flight;
import org.apache.arrow.flight.impl.FlightServiceGrpc.FlightServiceImplBase;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.dictionary.DictionaryProvider.MapDictionaryProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;

/**
 * GRPC service implementation for a flight server.
 */
class FlightService extends FlightServiceImplBase {

  private static final Logger logger = LoggerFactory.getLogger(FlightService.class);
  private static final int PENDING_REQUESTS = 5;

  private final BufferAllocator allocator;
  private final FlightProducer producer;
  private final ServerAuthHandler authHandler;
  private final ExecutorService executors;

  FlightService(BufferAllocator allocator, FlightProducer producer, ServerAuthHandler authHandler,
      ExecutorService executors) {
    this.allocator = allocator;
    this.producer = producer;
    this.authHandler = authHandler;
    this.executors = new ContextPropagatingExecutorService(executors);
  }

  private CallContext makeContext(ServerCallStreamObserver<?> responseObserver) {
    return new CallContext(AuthConstants.PEER_IDENTITY_KEY.get(), responseObserver::isCancelled);
  }

  @Override
  public StreamObserver<Flight.HandshakeRequest> handshake(StreamObserver<Flight.HandshakeResponse> responseObserver) {
    return ServerAuthWrapper.wrapHandshake(authHandler, responseObserver, executors);
  }

  @Override
  public void listFlights(Flight.Criteria criteria, StreamObserver<Flight.FlightInfo> responseObserver) {
    final StreamPipe<FlightInfo, Flight.FlightInfo> listener = StreamPipe
        .wrap(responseObserver, FlightInfo::toProtocol, this::handleExceptionWithMiddleware);
    try {
      final CallContext context = makeContext((ServerCallStreamObserver<?>) responseObserver);
      producer.listFlights(context, new Criteria(criteria), listener);
    } catch (Exception ex) {
      listener.onError(ex);
    }
    // Do NOT call StreamPipe#onCompleted, as the FlightProducer implementation may be asynchronous
  }

  public void doGetCustom(Flight.Ticket ticket, StreamObserver<ArrowMessage> responseObserver) {
    final GetListener listener = new GetListener(responseObserver, this::handleExceptionWithMiddleware);
    try {
      producer.getStream(makeContext((ServerCallStreamObserver<?>) responseObserver), new Ticket(ticket), listener);
    } catch (Exception ex) {
      listener.error(ex);
    }
    // Do NOT call GetListener#completed, as the implementation of getStream may be asynchronous
  }

  @Override
  public void doAction(Flight.Action request, StreamObserver<Flight.Result> responseObserver) {
    final StreamPipe<Result, Flight.Result> listener = StreamPipe
        .wrap(responseObserver, Result::toProtocol, this::handleExceptionWithMiddleware);
    try {
      final CallContext context = makeContext((ServerCallStreamObserver<?>) responseObserver);
      producer.doAction(context, new Action(request), listener);
    } catch (Exception ex) {
      listener.onError(ex);
    }
    // Do NOT call StreamPipe#onCompleted, as the FlightProducer implementation may be asynchronous
  }

  @Override
  public void listActions(Flight.Empty request, StreamObserver<Flight.ActionType> responseObserver) {
    final StreamPipe<org.apache.arrow.flight.ActionType, Flight.ActionType> listener = StreamPipe
        .wrap(responseObserver, ActionType::toProtocol, this::handleExceptionWithMiddleware);
    try {
      final CallContext context = makeContext((ServerCallStreamObserver<?>) responseObserver);
      producer.listActions(context, listener);
    } catch (Exception ex) {
      listener.onError(ex);
    }
    // Do NOT call StreamPipe#onCompleted, as the FlightProducer implementation may be asynchronous
  }

  private static class GetListener implements ServerStreamListener {
    private ServerCallStreamObserver<ArrowMessage> responseObserver;
    private final Consumer<Throwable> errorHandler;
    private Runnable onCancelHandler = null;
    // null until stream started
    private volatile VectorUnloader unloader;
    private boolean completed;

    public GetListener(StreamObserver<ArrowMessage> responseObserver, Consumer<Throwable> errorHandler) {
      super();
      this.errorHandler = errorHandler;
      this.completed = false;
      this.responseObserver = (ServerCallStreamObserver<ArrowMessage>) responseObserver;
      this.responseObserver.setOnCancelHandler(this::onCancel);
      this.responseObserver.disableAutoInboundFlowControl();
    }

    private void onCancel() {
      logger.debug("Stream cancelled by client.");
      if (onCancelHandler != null) {
        onCancelHandler.run();
      }
    }

    @Override
    public void setOnCancelHandler(Runnable handler) {
      this.onCancelHandler = handler;
    }

    @Override
    public boolean isReady() {
      return responseObserver.isReady();
    }

    @Override
    public boolean isCancelled() {
      return responseObserver.isCancelled();
    }

    @Override
    public void start(VectorSchemaRoot root) {
      start(root, new MapDictionaryProvider());
    }

    @Override
    public void start(VectorSchemaRoot root, DictionaryProvider provider) {
      unloader = new VectorUnloader(root, true, true);

      try {
        DictionaryUtils.generateSchemaMessages(root.getSchema(), null, provider, responseObserver::onNext);
      } catch (Exception e) {
        // Only happens if closing buffers somehow fails - indicates application is an unknown state so propagate
        // the exception
        throw new RuntimeException("Could not generate and send all schema messages", e);
      }
    }

    @Override
    public void putNext() {
      putNext(null);
    }

    @Override
    public void putNext(ArrowBuf metadata) {
      Preconditions.checkNotNull(unloader);
      // close is a no-op if the message has been written to gRPC, otherwise frees the associated buffers
      // in some code paths (e.g. if the call is cancelled), gRPC does not write the message, so we need to clean up
      // ourselves. Normally, writing the ArrowMessage will transfer ownership of the data to gRPC/Netty.
      try (final ArrowMessage message = new ArrowMessage(unloader.getRecordBatch(), metadata)) {
        responseObserver.onNext(message);
      } catch (Exception e) {
        // This exception comes from ArrowMessage#close, not responseObserver#onNext.
        // Generally this should not happen - ArrowMessage's implementation only closes non-throwing things.
        // The user can't reasonably do anything about this, but if something does throw, we shouldn't let
        // execution continue since other state (e.g. allocators) may be in an odd state.
        throw new RuntimeException("Could not free ArrowMessage", e);
      }
    }

    @Override
    public void error(Throwable ex) {
      if (!completed) {
        completed = true;
        responseObserver.onError(StatusUtils.toGrpcException(ex));
      } else {
        errorHandler.accept(ex);
      }
    }

    @Override
    public void completed() {
      if (unloader == null) {
        throw new IllegalStateException("Can't complete stream before starting it");
      }
      if (!completed) {
        completed = true;
        responseObserver.onCompleted();
      } else {
        errorHandler.accept(new IllegalStateException("Tried to complete already-completed call"));
      }
    }

  }

  public StreamObserver<ArrowMessage> doPutCustom(final StreamObserver<Flight.PutResult> responseObserverSimple) {
    ServerCallStreamObserver<Flight.PutResult> responseObserver =
        (ServerCallStreamObserver<Flight.PutResult>) responseObserverSimple;
    responseObserver.disableAutoInboundFlowControl();
    responseObserver.request(1);

    final FlightStream fs = new FlightStream(allocator, PENDING_REQUESTS, (String message, Throwable cause) -> {
      responseObserver.onError(Status.CANCELLED.withCause(cause).withDescription(message).asException());
    }, responseObserver::request);
    final StreamObserver<ArrowMessage> observer = fs.asObserver();
    executors.submit(() -> {
      final StreamPipe<PutResult, Flight.PutResult> ackStream = StreamPipe
          .wrap(responseObserver, PutResult::toProtocol, this::handleExceptionWithMiddleware);
      try {
        producer.acceptPut(makeContext(responseObserver), fs, ackStream).run();
      } catch (Exception ex) {
        ackStream.onError(ex);
      } finally {
        // ARROW-6136: Close the stream if and only if acceptPut hasn't closed it itself
        // We don't do this for other streams since the implementation may be asynchronous
        ackStream.ensureCompleted();
        try {
          fs.close();
        } catch (Exception e) {
          handleExceptionWithMiddleware(e);
        }
      }
    });

    return observer;
  }

  @Override
  public void getFlightInfo(Flight.FlightDescriptor request, StreamObserver<Flight.FlightInfo> responseObserver) {
    final FlightInfo info;
    try {
      info = producer
          .getFlightInfo(makeContext((ServerCallStreamObserver<?>) responseObserver), new FlightDescriptor(request));
    } catch (Exception ex) {
      // Don't capture exceptions from onNext or onCompleted with this block - because then we can't call onError
      responseObserver.onError(StatusUtils.toGrpcException(ex));
      return;
    }
    responseObserver.onNext(info.toProtocol());
    responseObserver.onCompleted();
  }

  /**
   * Broadcast the given exception to all registered middleware.
   */
  private void handleExceptionWithMiddleware(Throwable t) {
    final Map<Key<?>, FlightServerMiddleware> middleware = ServerInterceptorAdapter.SERVER_MIDDLEWARE_KEY.get();
    if (middleware == null) {
      logger.error("Uncaught exception in Flight method body", t);
      return;
    }
    middleware.forEach((k, v) -> v.onCallErrored(t));
  }

  @Override
  public void getSchema(Flight.FlightDescriptor request, StreamObserver<Flight.SchemaResult> responseObserver) {
    try {
      SchemaResult result = producer
              .getSchema(makeContext((ServerCallStreamObserver<?>) responseObserver),
                      new FlightDescriptor(request));
      responseObserver.onNext(result.toProtocol());
      responseObserver.onCompleted();
    } catch (Exception ex) {
      responseObserver.onError(StatusUtils.toGrpcException(ex));
    }
  }

  /**
   * Call context for the service.
   */
  static class CallContext implements FlightProducer.CallContext {

    private final String peerIdentity;
    private final BooleanSupplier isCancelled;

    CallContext(final String peerIdentity, BooleanSupplier isCancelled) {
      this.peerIdentity = peerIdentity;
      this.isCancelled = isCancelled;
    }

    @Override
    public String peerIdentity() {
      return peerIdentity;
    }

    @Override
    public boolean isCancelled() {
      return this.isCancelled.getAsBoolean();
    }

    @Override
    public <T extends FlightServerMiddleware> T getMiddleware(Key<T> key) {
      final Map<Key<?>, FlightServerMiddleware> middleware = ServerInterceptorAdapter.SERVER_MIDDLEWARE_KEY.get();
      if (middleware == null) {
        return null;
      }
      final FlightServerMiddleware m = middleware.get(key);
      if (m == null) {
        return null;
      }
      @SuppressWarnings("unchecked") final T result = (T) m;
      return result;
    }

    @Override
    public Map<Key<?>, FlightServerMiddleware> getMiddleware() {
      final Map<Key<?>, FlightServerMiddleware> middleware = ServerInterceptorAdapter.SERVER_MIDDLEWARE_KEY.get();
      if (middleware == null) {
        return Collections.emptyMap();
      }
      // This is an unmodifiable map
      return middleware;
    }
  }
}
