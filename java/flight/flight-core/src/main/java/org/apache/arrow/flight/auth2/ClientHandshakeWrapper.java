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

package org.apache.arrow.flight.auth2;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.grpc.StatusUtils;
import org.apache.arrow.flight.impl.Flight.HandshakeRequest;
import org.apache.arrow.flight.impl.Flight.HandshakeResponse;
import org.apache.arrow.flight.impl.FlightServiceGrpc.FlightServiceStub;

import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

/**
 * Utility class for executing a handshake with a FlightServer.
 */
public class ClientHandshakeWrapper {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ClientHandshakeWrapper.class);

  /**
   * Do handshake for a client.  The stub will be authenticated after this method returns.
   *
   * @param stub The service stub.
   */
  public static void doClientHandshake(FlightServiceStub stub) {
    final HandshakeObserver observer = new HandshakeObserver();
    try {
      observer.requestObserver = stub.handshake(observer);
      observer.requestObserver.onNext(HandshakeRequest.newBuilder().build());
      observer.requestObserver.onCompleted();
      try {
        if (!observer.completed.get()) {
          // TODO: ARROW-5681
          throw CallStatus.UNAUTHENTICATED.toRuntimeException();
        }
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        throw ex;
      } catch (ExecutionException ex) {
        final FlightRuntimeException wrappedException = StatusUtils.fromThrowable(ex.getCause());
        logger.error("Failed on completing future", wrappedException);
        throw wrappedException;
      }
    } catch (StatusRuntimeException sre) {
      logger.error("Failed with SREe", sre);
      throw StatusUtils.fromGrpcRuntimeException(sre);
    } catch (Throwable ex) {
      logger.error("Failed with unknown", ex);
      if (ex instanceof FlightRuntimeException) {
        throw (FlightRuntimeException) ex;
      }
      throw StatusUtils.fromThrowable(ex);
    }
  }

  private static class HandshakeObserver implements StreamObserver<HandshakeResponse> {

    private volatile StreamObserver<HandshakeRequest> requestObserver;
    private final CompletableFuture<Boolean> completed;

    public HandshakeObserver() {
      super();
      completed = new CompletableFuture<>();
    }

    @Override
    public void onNext(HandshakeResponse value) {
    }

    @Override
    public void onError(Throwable t) {
      completed.completeExceptionally(t);
    }

    @Override
    public void onCompleted() {
      completed.complete(true);
    }
  }

}
