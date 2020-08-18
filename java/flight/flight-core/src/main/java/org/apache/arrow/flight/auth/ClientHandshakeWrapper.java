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

package org.apache.arrow.flight.auth;

import java.util.concurrent.CompletableFuture;

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

  /**
   * Do handshake for a client.  The stub will be authenticated after this method returns.
   *
   * @param stub The service stub.
   */
  public static void doClientHandshake(FlightServiceStub stub) {
    final HandshakeObserver observer = new HandshakeObserver();
    try {
      observer.responseObserver = stub.handshake(observer);
      observer.responseObserver.onCompleted();
    } catch (StatusRuntimeException sre) {
      throw StatusUtils.fromGrpcRuntimeException(sre);
    }
  }

  private static class HandshakeObserver implements StreamObserver<HandshakeResponse> {

    private volatile StreamObserver<HandshakeRequest> responseObserver;
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
