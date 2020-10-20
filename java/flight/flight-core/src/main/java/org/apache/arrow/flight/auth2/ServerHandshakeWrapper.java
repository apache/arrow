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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.arrow.flight.impl.Flight.HandshakeRequest;
import org.apache.arrow.flight.impl.Flight.HandshakeResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.stub.StreamObserver;

/**
 * Contains utility methods for integrating authorization into a GRPC stream.
 */
public class ServerHandshakeWrapper {
  private static final Logger LOGGER = LoggerFactory.getLogger(ServerHandshakeWrapper.class);

  /**
   * Wrap the auth handler for handshake purposes.
   *
   * @param responseObserver Observer for handshake response
   * @param executors        ExecutorService
   * @return AuthObserver
   */
  public static StreamObserver<HandshakeRequest> wrapHandshake(
      StreamObserver<HandshakeResponse> responseObserver, ExecutorService executors) {

    // stream started.
    final RequestObserver observer = new RequestObserver();
    final Runnable r = () -> {
      responseObserver.onNext(HandshakeResponse.newBuilder().build());
      responseObserver.onCompleted();
    };
    observer.future = executors.submit(r);
    return observer;
  }

  private static class RequestObserver implements StreamObserver<HandshakeRequest> {

    private volatile Future<?> future;

    public RequestObserver() {
      super();
    }

    @Override
    public void onNext(HandshakeRequest value) {
      LOGGER.debug("Got HandshakeRequest");
    }

    @Override
    public void onError(Throwable t) {
      LOGGER.error("Error", t);
      while (future == null) {/* busy wait */}
      future.cancel(true);
    }

    @Override
    public void onCompleted() {
      LOGGER.debug("Got HandshakeRequest.onCompleted");
    }
  }
}
