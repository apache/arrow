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

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.arrow.flight.auth.ServerAuthHandler;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.Assert;
import org.junit.Test;

import com.google.protobuf.Empty;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;

/**
 * Unit test which adds 2 services to same server end point.
 */
public class TestFlightGrpcUtils {

  @Test(expected = RuntimeException.class)
  public void testGrpcConnection() throws IOException {

    final BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
    final NoOpFlightProducer producer = new NoOpFlightProducer();
    final ServerAuthHandler authHandler = ServerAuthHandler.NO_OP;
    final ExecutorService exec = Executors.newCachedThreadPool();
    final FlightBindingService flightBindingService = FlightGrpcUtils.getFlightService(allocator, producer,
            authHandler, exec);

    final String serverName = InProcessServerBuilder.generateName();
    final Server server = InProcessServerBuilder.forName(serverName)
            .directExecutor()
            .addService(flightBindingService)
            .addService(new TestServiceAdapter())
            .build();

    server.start();

    final ManagedChannel managedChannel = InProcessChannelBuilder.forName(serverName)
            .directExecutor()
            .build();

    final FlightClient flightClient = FlightGrpcUtils.getFlightClient(allocator, managedChannel);

    final Iterable<ActionType> actionTypes = flightClient.listActions();
    actionTypes.forEach(actionType -> System.out.println(actionType.toString()));

    final TestServiceGrpc.TestServiceBlockingStub blockingStub = TestServiceGrpc.newBlockingStub(managedChannel);
    Assert.assertEquals(Empty.newBuilder().build(), blockingStub.test(Empty.newBuilder().build()));
  }

  private class TestServiceAdapter extends TestServiceGrpc.TestServiceImplBase {

    @Override
    public void test(Empty request, StreamObserver<Empty> responseObserver) {
      responseObserver.onNext(Empty.newBuilder().build());
      responseObserver.onCompleted();
    }
  }
}

