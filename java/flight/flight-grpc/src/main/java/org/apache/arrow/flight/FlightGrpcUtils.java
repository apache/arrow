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

import java.util.Collections;
import java.util.concurrent.ExecutorService;

import org.apache.arrow.flight.auth.ServerAuthHandler;
import org.apache.arrow.memory.BufferAllocator;

import io.grpc.BindableService;
import io.grpc.ManagedChannel;

/**
 * Exposes Flight GRPC service & client.
 */
public class FlightGrpcUtils {

  private FlightGrpcUtils() {}

  /**
   * Creates a Flight service.
   * @param allocator Memory allocator
   * @param producer Specifies the service api
   * @param authHandler Authentication handler
   * @param executor Executor service
   * @return FlightBindingService
   */
  public static BindableService createFlightService(BufferAllocator allocator, FlightProducer producer,
                                                    ServerAuthHandler authHandler, ExecutorService executor) {
    return new FlightBindingService(allocator, producer, authHandler, executor);
  }

  /**
   * Creates a Flight client.
   * @param incomingAllocator  Memory allocator
   * @param channel provides a connection to a gRPC server
   * @return FlightClient
   */
  public static FlightClient createFlightClient(BufferAllocator incomingAllocator, ManagedChannel channel) {
    return new FlightClient(incomingAllocator, channel, Collections.emptyList());
  }
}
