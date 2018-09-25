/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.flight.auth;

import java.util.Iterator;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.arrow.flight.auth.ClientAuthHandler.ClientAuthSender;
import org.apache.arrow.flight.impl.Flight.HandshakeRequest;
import org.apache.arrow.flight.impl.Flight.HandshakeResponse;
import org.apache.arrow.flight.impl.FlightServiceGrpc.FlightServiceStub;

import com.google.common.base.Throwables;
import com.google.protobuf.ByteString;

import io.grpc.stub.StreamObserver;

public class ClientAuthWrapper {

  /**
   * Do client auth for a client.
   * @param authHandler The handler to use.
   * @param stub The service stub.
   * @return The token if auth was successful.
   */
  public static byte[] doClientAuth(ClientAuthHandler authHandler, FlightServiceStub stub) {

    AuthObserver observer = new AuthObserver();
    observer.responseObserver = stub.handshake(observer);
    byte[] bytes = authHandler.authenticate(observer.sender, observer.iter);
    observer.responseObserver.onCompleted();
    return bytes;
  }

  private static class AuthObserver implements StreamObserver<HandshakeResponse> {

    private volatile StreamObserver<HandshakeRequest> responseObserver;
    private final LinkedBlockingQueue<byte[]> messages = new LinkedBlockingQueue<>();
    private final AuthSender sender = new AuthSender();
    private volatile boolean completed = false;
    private Throwable ex = null;

    public AuthObserver() {
      super();
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
        while (ex == null && (!completed || !messages.isEmpty())) {
          byte[] bytes = messages.poll();
          if (bytes == null) {
            // busy wait.
            continue;
          } else {
            return bytes;
          }
        }

        if (ex != null) {
          throw Throwables.propagate(ex);
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
      ex = t;
    }

    private class AuthSender implements ClientAuthSender {

      @Override
      public void send(byte[] payload) {
        responseObserver.onNext(HandshakeRequest.newBuilder()
            .setPayload(ByteString.copyFrom(payload))
            .build());
      }

      @Override
      public void onError(String message, Throwable cause) {
        responseObserver.onError(cause);
      }

    }

    @Override
    public void onCompleted() {
      completed = true;
    }
  }

}
