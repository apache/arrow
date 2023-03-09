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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import org.apache.arrow.flight.TestBasicOperation.Producer;
import org.apache.arrow.flight.auth.ServerAuthHandler;
import org.apache.arrow.flight.impl.FlightServiceGrpc;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import io.grpc.MethodDescriptor;
import io.grpc.ServerServiceDefinition;
import io.grpc.netty.NettyServerBuilder;

public class TestServerOptions {

  @Test
  public void builderConsumer() throws Exception {
    final AtomicBoolean consumerCalled = new AtomicBoolean();
    final Consumer<NettyServerBuilder> consumer = (builder) -> consumerCalled.set(true);

    try (
        BufferAllocator a = new RootAllocator(Long.MAX_VALUE);
        Producer producer = new Producer(a);
        FlightServer s = FlightServer.builder(a, forGrpcInsecure(LOCALHOST, 0), producer)
            .transportHint("grpc.builderConsumer", consumer).build().start()
    ) {
      Assertions.assertTrue(consumerCalled.get());
    }
  }

  /**
   * Make sure that if Flight supplies a default executor to gRPC, then it is closed along with the server.
   */
  @Test
  public void defaultExecutorClosed() throws Exception {
    final ExecutorService executor;
    try (
        BufferAllocator a = new RootAllocator(Long.MAX_VALUE);
        FlightServer server = FlightServer.builder(a, forGrpcInsecure(LOCALHOST, 0), new NoOpFlightProducer())
            .build().start()
    ) {
      assertNotNull(server.grpcExecutor);
      executor = server.grpcExecutor;
    }
    Assertions.assertTrue(executor.isShutdown());
  }

  /**
   * Make sure that if the user provides an executor to gRPC, then Flight does not close it.
   */
  @Test
  public void suppliedExecutorNotClosed() throws Exception {
    final ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      try (
          BufferAllocator a = new RootAllocator(Long.MAX_VALUE);
          FlightServer server = FlightServer.builder(a, forGrpcInsecure(LOCALHOST, 0), new NoOpFlightProducer())
              .executor(executor)
              .build().start()
      ) {
        Assertions.assertNull(server.grpcExecutor);
      }
      Assertions.assertFalse(executor.isShutdown());
    } finally {
      executor.shutdown();
    }
  }

  @Test
  public void domainSocket() throws Exception {
    Assumptions.assumeTrue(FlightTestUtil.isNativeTransportAvailable(), "We have a native transport available");
    final File domainSocket = File.createTempFile("flight-unit-test-", ".sock");
    Assertions.assertTrue(domainSocket.delete());
    // Domain socket paths have a platform-dependent limit. Set a conservative limit and skip the test if the temporary
    // file name is too long. (We do not assume a particular platform-dependent temporary directory path.)
    Assumptions.assumeTrue(domainSocket.getAbsolutePath().length() < 100, "The domain socket path is not too long");
    final Location location = Location.forGrpcDomainSocket(domainSocket.getAbsolutePath());
    try (
        BufferAllocator a = new RootAllocator(Long.MAX_VALUE);
        Producer producer = new Producer(a);
        FlightServer s = FlightServer.builder(a, location, producer).build().start();
    ) {
      try (FlightClient c = FlightClient.builder(a, location).build()) {
        try (FlightStream stream = c.getStream(new Ticket(new byte[0]))) {
          VectorSchemaRoot root = stream.getRoot();
          IntVector iv = (IntVector) root.getVector("c1");
          int value = 0;
          while (stream.next()) {
            for (int i = 0; i < root.getRowCount(); i++) {
              Assertions.assertEquals(value, iv.get(i));
              value++;
            }
          }
        }
      }
    }
  }

  @Test
  public void checkReflectionMetadata() {
    // This metadata is needed for gRPC reflection to work.
    final ExecutorService executorService = Executors.newSingleThreadExecutor();
    try (final BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE)) {
      final FlightBindingService service = new FlightBindingService(allocator, new NoOpFlightProducer(),
          ServerAuthHandler.NO_OP, executorService);
      final ServerServiceDefinition definition = service.bindService();
      assertEquals(FlightServiceGrpc.getServiceDescriptor().getSchemaDescriptor(),
          definition.getServiceDescriptor().getSchemaDescriptor());

      final Map<String, MethodDescriptor<?, ?>> definedMethods = new HashMap<>();
      final Map<String, MethodDescriptor<?, ?>> serviceMethods = new HashMap<>();

      // Make sure that the reflection metadata object is identical across all the places where it's accessible
      definition.getMethods().forEach(
          method -> definedMethods.put(method.getMethodDescriptor().getFullMethodName(), method.getMethodDescriptor()));
      definition.getServiceDescriptor().getMethods().forEach(
          method -> serviceMethods.put(method.getFullMethodName(), method));

      for (final MethodDescriptor<?, ?> descriptor : FlightServiceGrpc.getServiceDescriptor().getMethods()) {
        final String methodName = descriptor.getFullMethodName();
        Assertions.assertTrue(definedMethods.containsKey(methodName),
            "Method is missing from ServerServiceDefinition: " + methodName);
        Assertions.assertTrue(definedMethods.containsKey(methodName),
            "Method is missing from ServiceDescriptor: " + methodName);

        assertEquals(descriptor.getSchemaDescriptor(), definedMethods.get(methodName).getSchemaDescriptor());
        assertEquals(descriptor.getSchemaDescriptor(), serviceMethods.get(methodName).getSchemaDescriptor());
      }
    } finally {
      executorService.shutdown();
    }
  }
}
