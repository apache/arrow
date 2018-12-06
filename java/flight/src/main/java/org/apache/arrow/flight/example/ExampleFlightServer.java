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

package org.apache.arrow.flight.example;

import java.io.IOException;

import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.auth.ServerAuthHandler;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;

/**
 * An Example Flight Server that provides access to the InMemoryStore.
 */
public class ExampleFlightServer implements AutoCloseable {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ExampleFlightServer.class);

  private final FlightServer flightServer;
  private final Location location;
  private final BufferAllocator allocator;
  private final InMemoryStore mem;

  public ExampleFlightServer(BufferAllocator allocator, Location location) {
    this.allocator = allocator.newChildAllocator("flight-server", 0, Long.MAX_VALUE);
    this.location = location;
    this.mem = new InMemoryStore(this.allocator, location);
    this.flightServer = new FlightServer(allocator, location.getPort(), mem, ServerAuthHandler.NO_OP);
  }

  public Location getLocation() {
    return location;
  }

  public void start() throws IOException {
    flightServer.start();
  }

  public InMemoryStore getStore() {
    return mem;
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(mem, flightServer, allocator);
  }

  public static void main(String[] args) throws Exception {
    final BufferAllocator a = new RootAllocator(Long.MAX_VALUE);
    final ExampleFlightServer efs = new ExampleFlightServer(a, new Location("localhost", 12233));
    efs.start();
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        System.out.println("\nExiting...");
        AutoCloseables.close(efs, a);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }));
    while (true) {
      Thread.sleep(30000);
    }
  }
}
