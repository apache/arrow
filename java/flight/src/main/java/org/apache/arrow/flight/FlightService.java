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

import java.util.concurrent.ExecutorService;
import java.util.function.BooleanSupplier;

import org.apache.arrow.flight.FlightProducer.ServerStreamListener;
import org.apache.arrow.flight.auth.AuthConstants;
import org.apache.arrow.flight.auth.ServerAuthHandler;
import org.apache.arrow.flight.auth.ServerAuthWrapper;
import org.apache.arrow.flight.grpc.StatusUtils;
import org.apache.arrow.flight.impl.Flight;
import org.apache.arrow.flight.impl.Flight.ActionType;
import org.apache.arrow.flight.impl.Flight.Empty;
import org.apache.arrow.flight.impl.Flight.HandshakeRequest;
import org.apache.arrow.flight.impl.Flight.HandshakeResponse;
import org.apache.arrow.flight.impl.FlightServiceGrpc.FlightServiceImplBase;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.dictionary.DictionaryProvider.MapDictionaryProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import io.netty.buffer.ArrowBuf;

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
    this.executors = executors;
  }

  @Override
  public StreamObserver<HandshakeRequest> handshake(StreamObserver<HandshakeResponse> responseObserver) {
    return ServerAuthWrapper.wrapHandshake(authHandler, responseObserver, executors);
  }

  @Override
  public void listFlights(Flight.Criteria criteria, StreamObserver<Flight.FlightInfo> responseObserver) {
    try {
      producer.listFlights(makeContext((ServerCallStreamObserver<?>) responseObserver), new Criteria(criteria),
          StreamPipe.wrap(responseObserver, FlightInfo::toProtocol));
    } catch (Exception ex) {
      responseObserver.onError(StatusUtils.toGrpcException(ex));
    }
  }

  private CallContext makeContext(ServerCallStreamObserver<?> responseObserver) {
    return new CallContext(AuthConstants.PEER_IDENTITY_KEY.get(),
        responseObserver::isCancelled);
  }

  public void doGetCustom(Flight.Ticket ticket, StreamObserver<ArrowMessage> responseObserver) {
    try {
      producer.getStream(makeContext((ServerCallStreamObserver<?>) responseObserver), new Ticket(ticket),
          new GetListener(responseObserver));
    } catch (Exception ex) {
      responseObserver.onError(StatusUtils.toGrpcException(ex));
    }
  }

  @Override
  public void doAction(Flight.Action request, StreamObserver<Flight.Result> responseObserver) {
    try {
      producer.doAction(makeContext((ServerCallStreamObserver<?>) responseObserver), new Action(request),
          StreamPipe.wrap(responseObserver, Result::toProtocol));
    } catch (Exception ex) {
      responseObserver.onError(StatusUtils.toGrpcException(ex));
    }
  }

  @Override
  public void listActions(Empty request, StreamObserver<ActionType> responseObserver) {
    try {
      producer.listActions(makeContext((ServerCallStreamObserver<?>) responseObserver),
          StreamPipe.wrap(responseObserver, t -> t.toProtocol()));
    } catch (Exception ex) {
      responseObserver.onError(StatusUtils.toGrpcException(ex));
    }
  }

  private static class GetListener implements ServerStreamListener {
    private ServerCallStreamObserver<ArrowMessage> responseObserver;
    private volatile VectorUnloader unloader;

    public GetListener(StreamObserver<ArrowMessage> responseObserver) {
      super();
      this.responseObserver = (ServerCallStreamObserver<ArrowMessage>) responseObserver;
      this.responseObserver.setOnCancelHandler(() -> onCancel());
      this.responseObserver.disableAutoInboundFlowControl();
    }

    private void onCancel() {
      logger.debug("Stream cancelled by client.");
    }

    @Override
    public boolean isReady() {

      return responseObserver.isReady();
    }

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

      DictionaryUtils.generateSchemaMessages(root.getSchema(), null, provider, responseObserver::onNext);
    }

    @Override
    public void putNext() {
      putNext(null);
    }

    @Override
    public void putNext(ArrowBuf metadata) {
      Preconditions.checkNotNull(unloader);
      responseObserver.onNext(new ArrowMessage(unloader.getRecordBatch(), metadata));
    }

    @Override
    public void error(Throwable ex) {
      responseObserver.onError(StatusUtils.toGrpcException(ex));
    }

    @Override
    public void completed() {
      responseObserver.onCompleted();
    }

  }

  public StreamObserver<ArrowMessage> doPutCustom(final StreamObserver<Flight.PutResult> responseObserverSimple) {
    ServerCallStreamObserver<Flight.PutResult> responseObserver =
        (ServerCallStreamObserver<Flight.PutResult>) responseObserverSimple;
    responseObserver.disableAutoInboundFlowControl();
    responseObserver.request(1);

    // Set a default metadata listener that does nothing. Service implementations should call
    // FlightStream#setMetadataListener before returning a Runnable if they want to receive metadata.
    FlightStream fs = new FlightStream(allocator, PENDING_REQUESTS, (String message, Throwable cause) -> {
      responseObserver.onError(Status.CANCELLED.withCause(cause).withDescription(message).asException());
    }, responseObserver::request);
    executors.submit(() -> {
      try {
        producer.acceptPut(makeContext(responseObserver), fs,
            StreamPipe.wrap(responseObserver, PutResult::toProtocol)).run();
        responseObserver.onCompleted();
      } catch (Exception ex) {
        logger.error("Failed to process custom put.", ex);
        responseObserver.onError(StatusUtils.toGrpcException(ex));
        // The client may have terminated, so the exception here is effectively swallowed.
        // Log the error as well so -something- makes it to the developer.
        logger.error("Exception handling DoPut", ex);
      }
      try {
        fs.close();
      } catch (Exception e) {
        logger.error("Exception closing Flight stream", e);
        throw new RuntimeException(e);
      }
    });

    return fs.asObserver();
  }

  @Override
  public void getFlightInfo(Flight.FlightDescriptor request, StreamObserver<Flight.FlightInfo> responseObserver) {
    try {
      FlightInfo info = producer
          .getFlightInfo(makeContext((ServerCallStreamObserver<?>) responseObserver), new FlightDescriptor(request));
      responseObserver.onNext(info.toProtocol());
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
  }
}
