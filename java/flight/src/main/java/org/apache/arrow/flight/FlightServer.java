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
import java.util.concurrent.TimeUnit;

import org.apache.arrow.flight.auth.ServerAuthHandler;
import org.apache.arrow.flight.auth.ServerAuthInterceptor;
import org.apache.arrow.flight.impl.Flight.FlightGetInfo;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerInterceptors;

public class FlightServer implements AutoCloseable {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FlightServer.class);

  private final Server server;

  /** The maximum size of an individual gRPC message. This effectively disables the limit. */
  static final int MAX_GRPC_MESSAGE_SIZE = Integer.MAX_VALUE;

  public FlightServer(
      BufferAllocator allocator,
      int port,
      FlightProducer producer,
      ServerAuthHandler authHandler) {
    this.server = ServerBuilder.forPort(port)
        .maxInboundMessageSize(MAX_GRPC_MESSAGE_SIZE)
        .addService(
            ServerInterceptors.intercept(
                new FlightBindingService(allocator, producer, authHandler),
                new ServerAuthInterceptor(authHandler)))
        .build();
  }

  public FlightServer start() throws IOException {
    server.start();
    return this;
  }

  public int getPort() {
    return server.getPort();
  }

  public void close() throws InterruptedException {
    server.shutdown();
    final boolean terminated = server.awaitTermination(3000, TimeUnit.MILLISECONDS);
    if (terminated) {
      logger.debug("Server was terminated within 3s");
      return;
    }

    // get more aggressive in termination.
    server.shutdownNow();

    int count = 0;
    while (!server.isTerminated() & count < 30) {
      count++;
      logger.debug("Waiting for termination");
      Thread.sleep(100);
    }

    if (!server.isTerminated()) {
      logger.warn("Couldn't shutdown server, resources likely will be leaked.");
    }
  }

  public interface OutputFlight {
    void sendData(int count);

    void done();

    void fail(Throwable t);
  }

  public interface FlightServerHandler {

    public FlightGetInfo getFlightInfo(String descriptor) throws Exception;

    public OutputFlight setupFlight(VectorSchemaRoot root);

  }

}
