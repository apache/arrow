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
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.arrow.flight.auth.ClientAuthHandler.ClientAuthSender;
import org.apache.arrow.flight.impl.Flight.HandshakeRequest;
import org.apache.arrow.flight.impl.Flight.HandshakeResponse;
import org.apache.arrow.flight.impl.FlightServiceGrpc.FlightServiceStub;

import com.google.protobuf.ByteString;

import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

/**
 * Utility class for performing authorization over using a GRPC stub.
 */
public class ClientAuthWrapper {

  /**
   * Do client auth for a client.  The stub will be authenticated after this method returns.
   *
   * @param authHandler The handler to use.
   * @param stub The service stub.
   */
  public static void doClientAuth(ClientAuthHandler authHandler, FlightServiceStub stub) {
    AuthObserver observer = new AuthObserver();
    observer.responseObserver = stub.handshake(observer);
    authHandler.authenticate(observer.sender, observer.iter);
    if (!observer.sender.errored) {
      observer.responseObserver.onCompleted();
    }
    try {
      if (!observer.completed.get()) {
        // TODO: ARROW-5681
        throw new RuntimeException("Unauthenticated");
      }
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  private static class AuthObserver implements StreamObserver<HandshakeResponse> {

    private volatile StreamObserver<HandshakeRequest> responseObserver;
    private final LinkedBlockingQueue<byte[]> messages = new LinkedBlockingQueue<>();
    private final AuthSender sender = new AuthSender();
    private CompletableFuture<Boolean> completed;

    public AuthObserver() {
      super();
      completed = new CompletableFuture<>();
    }

    @Override
    public void onNext(HandshakeResponse value) {
      ByteString payload = value.getPayload();
      if (payload != null) {
        messages.add(payload.toByteArray());
      }
    }

    private Iterator<byte[]> iter = new Iterator<byte[]>() {

      @Override
      public byte[] next() {
        while (!completed.isDone() || !messages.isEmpty()) {
          byte[] bytes = messages.poll();
          if (bytes == null) {
            // busy wait.
            continue;
          } else {
            return bytes;
          }
        }

        if (completed.isCompletedExceptionally()) {
          // Preserve prior exception behavior
          // TODO: with ARROW-5681, throw an appropriate Flight exception if gRPC raised an exception
          try {
            completed.get();
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          } catch (ExecutionException e) {
            if (e.getCause() instanceof StatusRuntimeException) {
              throw (StatusRuntimeException) e.getCause();
            }
            throw new RuntimeException(e);
          }
        }

        throw new IllegalStateException("You attempted to retrieve messages after there were none.");
      }

      @Override
      public boolean hasNext() {
        return !messages.isEmpty();
      }
    };

    @Override
    public void onError(Throwable t) {
      completed.completeExceptionally(t);
    }

    private class AuthSender implements ClientAuthSender {

      private boolean errored = false;

      @Override
      public void send(byte[] payload) {
        responseObserver.onNext(HandshakeRequest.newBuilder()
            .setPayload(ByteString.copyFrom(payload))
            .build());
      }

      @Override
      public void onError(String message, Throwable cause) {
        this.errored = true;
        Objects.requireNonNull(cause);
        responseObserver.onError(cause);
      }

    }

    @Override
    public void onCompleted() {
      completed.complete(true);
    }
  }

}
