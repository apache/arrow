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

import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.auth.ServerAuthHandler.ServerAuthSender;
import org.apache.arrow.flight.grpc.StatusUtils;
import org.apache.arrow.flight.impl.Flight.HandshakeRequest;
import org.apache.arrow.flight.impl.Flight.HandshakeResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import io.grpc.stub.StreamObserver;

/**
 * Contains utility methods for integrating authorization into a GRPC stream.
 */
public class ServerAuthWrapper {
  private static final Logger LOGGER = LoggerFactory.getLogger(ServerAuthWrapper.class);

  /**
   * Wrap the auth handler for handshake purposes.
   *
   * @param authHandler Authentication handler
   * @param responseObserver Observer for handshake response
   * @param executors ExecutorService
   * @return AuthObserver
   */
  public static StreamObserver<HandshakeRequest> wrapHandshake(ServerAuthHandler authHandler,
      StreamObserver<HandshakeResponse> responseObserver, ExecutorService executors) {

    // stream started.
    AuthObserver observer = new AuthObserver(responseObserver);
    final Runnable r = () -> {
      try {
        if (authHandler.authenticate(observer.sender, observer.iter)) {
          responseObserver.onCompleted();
          return;
        }

        responseObserver.onError(StatusUtils.toGrpcException(CallStatus.UNAUTHENTICATED.toRuntimeException()));
      } catch (Exception ex) {
        LOGGER.error("Error during authentication", ex);
        responseObserver.onError(StatusUtils.toGrpcException(ex));
      }
    };
    observer.future = executors.submit(r);
    return observer;
  }

  private static class AuthObserver implements StreamObserver<HandshakeRequest> {

    private final StreamObserver<HandshakeResponse> responseObserver;
    private volatile Future<?> future;
    private volatile boolean completed = false;
    private final LinkedBlockingQueue<byte[]> messages = new LinkedBlockingQueue<>();
    private final AuthSender sender = new AuthSender();

    public AuthObserver(StreamObserver<HandshakeResponse> responseObserver) {
      super();
      this.responseObserver = responseObserver;
    }

    @Override
    public void onNext(HandshakeRequest value) {
      ByteString payload = value.getPayload();
      if (payload != null) {
        messages.add(payload.toByteArray());
      }
    }

    private Iterator<byte[]> iter = new Iterator<byte[]>() {

      @Override
      public byte[] next() {
        while (!completed || !messages.isEmpty()) {
          byte[] bytes = messages.poll();
          if (bytes == null) {
            //busy wait.
            continue;
          }
          return bytes;
        }
          throw new IllegalStateException("Requesting more messages than client sent.");
      }

      @Override
      public boolean hasNext() {
        return !messages.isEmpty();
      }
    };

    @Override
    public void onError(Throwable t) {
      completed = true;
      while (future == null) {/* busy wait */}
      future.cancel(true);
    }

    @Override
    public void onCompleted() {
      completed = true;
    }

    private class AuthSender implements ServerAuthSender {

      @Override
      public void send(byte[] payload) {
        responseObserver.onNext(HandshakeResponse.newBuilder()
            .setPayload(ByteString.copyFrom(payload))
            .build());
      }

      @Override
      public void onError(Throwable cause) {
        responseObserver.onError(StatusUtils.toGrpcException(cause));
      }

    }
  }

}
