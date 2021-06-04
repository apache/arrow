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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

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
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import io.grpc.MethodDescriptor;
import io.grpc.ServerServiceDefinition;
import io.grpc.netty.NettyServerBuilder;

@RunWith(JUnit4.class)
public class TestServerOptions {

  @Test
  public void builderConsumer() throws Exception {
    final AtomicBoolean consumerCalled = new AtomicBoolean();
    final Consumer<NettyServerBuilder> consumer = (builder) -> consumerCalled.set(true);

    try (
        BufferAllocator a = new RootAllocator(Long.MAX_VALUE);
        Producer producer = new Producer(a);
        FlightServer s =
            FlightTestUtil.getStartedServer(
                (location) -> FlightServer.builder(a, location, producer)
                    .transportHint("grpc.builderConsumer", consumer).build()
            )) {
      Assert.assertTrue(consumerCalled.get());
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
        FlightServer server =
            FlightTestUtil.getStartedServer(
                (location) -> FlightServer.builder(a, location, new NoOpFlightProducer())
                    .build()
            )) {
      assertNotNull(server.grpcExecutor);
      executor = server.grpcExecutor;
    }
    Assert.assertTrue(executor.isShutdown());
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
          FlightServer server =
              FlightTestUtil.getStartedServer(
                  (location) -> FlightServer.builder(a, location, new NoOpFlightProducer())
                      .executor(executor)
                      .build()
              )) {
        Assert.assertNull(server.grpcExecutor);
      }
      Assert.assertFalse(executor.isShutdown());
    } finally {
      executor.shutdown();
    }
  }

  @Test
  public void domainSocket() throws Exception {
    Assume.assumeTrue("We have a native transport available", FlightTestUtil.isNativeTransportAvailable());
    final File domainSocket = File.createTempFile("flight-unit-test-", ".sock");
    Assert.assertTrue(domainSocket.delete());
    // Domain socket paths have a platform-dependent limit. Set a conservative limit and skip the test if the temporary
    // file name is too long. (We do not assume a particular platform-dependent temporary directory path.)
    Assume.assumeTrue("The domain socket path is not too long", domainSocket.getAbsolutePath().length() < 100);
    final Location location = Location.forGrpcDomainSocket(domainSocket.getAbsolutePath());
    try (
        BufferAllocator a = new RootAllocator(Long.MAX_VALUE);
        Producer producer = new Producer(a);
        FlightServer s =
            FlightTestUtil.getStartedServer(
                (port) -> FlightServer.builder(a, location, producer).build()
            )) {
      try (FlightClient c = FlightClient.builder(a, location).build()) {
        try (FlightStream stream = c.getStream(new Ticket(new byte[0]))) {
          VectorSchemaRoot root = stream.getRoot();
          IntVector iv = (IntVector) root.getVector("c1");
          int value = 0;
          while (stream.next()) {
            for (int i = 0; i < root.getRowCount(); i++) {
              Assert.assertEquals(value, iv.get(i));
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
        Assert.assertTrue("Method is missing from ServerServiceDefinition: " + methodName,
            definedMethods.containsKey(methodName));
        Assert.assertTrue("Method is missing from ServiceDescriptor: " + methodName,
            definedMethods.containsKey(methodName));

        assertEquals(descriptor.getSchemaDescriptor(), definedMethods.get(methodName).getSchemaDescriptor());
        assertEquals(descriptor.getSchemaDescriptor(), serviceMethods.get(methodName).getSchemaDescriptor());
      }
    } finally {
      executorService.shutdown();
    }
  }
}
