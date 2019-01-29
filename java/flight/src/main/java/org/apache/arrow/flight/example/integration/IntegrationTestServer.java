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

package org.apache.arrow.flight.example.integration;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.Callable;

import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.ActionType;
import org.apache.arrow.flight.Criteria;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.auth.ServerAuthHandler;
import org.apache.arrow.flight.impl.Flight;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.JsonFileReader;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

class IntegrationTestServer {
  private final Options options;

  private IntegrationTestServer() {
    options = new Options();
    options.addOption("port", true, "The port to serve on.");
  }

  private void run(String[] args) throws Exception {
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = parser.parse(options, args, false);

    final BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
    final int port = Integer.parseInt(cmd.getOptionValue("port", "31337"));
    try (final IntegrationFlightProducer producer = new IntegrationFlightProducer(allocator);
         final FlightServer server = new FlightServer(allocator, port, producer, ServerAuthHandler.NO_OP)) {
      server.start();
      // Print out message for integration test script
      System.out.println("Server listening on localhost:" + server.getPort());
      while (true) {
        Thread.sleep(30000);
      }
    }
  }

  public static void main(String[] args) {
    try {
      new IntegrationTestServer().run(args);
    } catch (ParseException e) {
      IntegrationTestClient.fatalError("Error parsing arguments", e);
    } catch (Exception e) {
      IntegrationTestClient.fatalError("Runtime error", e);
    }
  }

  static class IntegrationFlightProducer implements FlightProducer, AutoCloseable {
    private final BufferAllocator allocator;

    IntegrationFlightProducer(BufferAllocator allocator) {
      this.allocator = allocator;
    }

    @Override
    public void close() {
      allocator.close();
    }

    @Override
    public void getStream(Ticket ticket, ServerStreamListener listener) {
      String path = new String(ticket.getBytes(), StandardCharsets.UTF_8);
      File inputFile = new File(path);
      try (JsonFileReader reader = new JsonFileReader(inputFile, allocator)) {
        Schema schema = reader.start();
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
          listener.start(root);
          while (reader.read(root)) {
            listener.putNext();
          }
          listener.completed();
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void listFlights(Criteria criteria, StreamListener<FlightInfo> listener) {
      listener.onCompleted();
    }

    @Override
    public FlightInfo getFlightInfo(FlightDescriptor descriptor) {
      if (descriptor.isCommand()) {
        throw new UnsupportedOperationException("Commands not supported.");
      }
      if (descriptor.getPath().size() < 1) {
        throw new IllegalArgumentException("Must provide a path.");
      }
      String path = descriptor.getPath().get(0);
      File inputFile = new File(path);
      try (JsonFileReader reader = new JsonFileReader(inputFile, allocator)) {
        Schema schema = reader.start();
        return new FlightInfo(schema, descriptor,
            Collections.singletonList(new FlightEndpoint(new Ticket(path.getBytes()),
            new Location("localhost", 31338))),
            0, 0);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public Callable<Flight.PutResult> acceptPut(FlightStream flightStream) {
      return null;
    }

    @Override
    public Result doAction(Action action) {
      return null;
    }

    @Override
    public void listActions(StreamListener<ActionType> listener) {
      listener.onCompleted();
    }
  }
}
