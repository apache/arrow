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

import static org.apache.arrow.flight.FlightTestUtil.LOCALHOST;
import static org.apache.arrow.flight.Location.forGrpcInsecure;
import static org.junit.jupiter.api.Assertions.fail;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Optional;

import org.apache.arrow.flight.impl.Flight;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.grpc.stub.ServerCallStreamObserver;

public class TestFlightService {

  private BufferAllocator allocator;

  @BeforeEach
  public void setup() {
    allocator = new RootAllocator(Long.MAX_VALUE);
  }

  @AfterEach
  public void cleanup() throws Exception {
    AutoCloseables.close(allocator);
  }

  @Test
  public void testFlightServiceWithNoAuthHandlerOrInterceptors() {
    // This test is for ARROW-10491. There was a bug where FlightService would try to access the RequestContext,
    // but the RequestContext was getting set to null because no interceptors were active to initialize it
    // when using FlightService directly rather than starting up a FlightServer.

    // Arrange
    final FlightProducer producer = new NoOpFlightProducer() {
      @Override
      public void getStream(CallContext context, Ticket ticket,
                            ServerStreamListener listener) {
        listener.completed();
      }
    };

    // This response observer notifies that the test failed if onError() is called.
    final ServerCallStreamObserver<ArrowMessage> observer = new ServerCallStreamObserver<ArrowMessage>() {
      @Override
      public boolean isCancelled() {
        return false;
      }

      @Override
      public void setOnCancelHandler(Runnable runnable) {

      }

      @Override
      public void setCompression(String s) {

      }

      @Override
      public boolean isReady() {
        return false;
      }

      @Override
      public void setOnReadyHandler(Runnable runnable) {

      }

      @Override
      public void disableAutoInboundFlowControl() {

      }

      @Override
      public void request(int i) {

      }

      @Override
      public void setMessageCompression(boolean b) {

      }

      @Override
      public void onNext(ArrowMessage arrowMessage) {

      }

      @Override
      public void onError(Throwable throwable) {
        fail(throwable);
      }

      @Override
      public void onCompleted() {

      }
    };
    final FlightService flightService = new FlightService(allocator, producer, null, null);

    // Act
    flightService.doGetCustom(Flight.Ticket.newBuilder().build(), observer);

    // fail() would have been called if an error happened during doGetCustom(), so this test passed.
  }

  @Test
  public void supportsNullSchemas() throws Exception
  {
    final FlightProducer producer = new NoOpFlightProducer() {
      @Override
      public FlightInfo getFlightInfo(CallContext context,
              FlightDescriptor descriptor) {
        return new FlightInfo(null, descriptor, Collections.emptyList(),
                0, 0, false, IpcOption.DEFAULT, "foo".getBytes(StandardCharsets.UTF_8));
      }
    };

    try (final FlightServer s =
            FlightServer.builder(allocator, forGrpcInsecure(LOCALHOST, 0), producer).build().start();
            final FlightClient client = FlightClient.builder(allocator, s.getLocation()).build()) {
      FlightInfo flightInfo = client.getInfo(FlightDescriptor.path("test"));
      Assertions.assertEquals(Optional.empty(), flightInfo.getSchemaOptional());
      Assertions.assertEquals(new Schema(Collections.emptyList()), flightInfo.getSchema());
      Assertions.assertArrayEquals(flightInfo.getAppMetadata(), "foo".getBytes(StandardCharsets.UTF_8));

      Exception e = Assertions.assertThrows(
          FlightRuntimeException.class,
          () -> client.getSchema(FlightDescriptor.path("test")));
      Assertions.assertEquals("No schema is present in FlightInfo", e.getMessage());
    }
  }
}
