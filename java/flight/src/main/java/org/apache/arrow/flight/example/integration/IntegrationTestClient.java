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
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightProducer.StreamListener;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.PutResult;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.JsonFileReader;
import org.apache.arrow.vector.util.Validator;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import io.netty.buffer.ArrowBuf;

/**
 * An Example Flight Server that provides access to the InMemoryStore.
 */
class IntegrationTestClient {
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(IntegrationTestClient.class);
  private final Options options;

  private IntegrationTestClient() {
    options = new Options();
    options.addOption("j", "json", true, "json file");
    options.addOption("host", true, "The host to connect to.");
    options.addOption("port", true, "The port to connect to.");
  }

  public static void main(String[] args) {
    try {
      new IntegrationTestClient().run(args);
    } catch (ParseException e) {
      fatalError("Invalid parameters", e);
    } catch (IOException e) {
      fatalError("Error accessing files", e);
    }
  }

  private static void fatalError(String message, Throwable e) {
    System.err.println(message);
    System.err.println(e.getMessage());
    LOGGER.error(message, e);
    System.exit(1);
  }

  private void run(String[] args) throws ParseException, IOException {
    final CommandLineParser parser = new DefaultParser();
    final CommandLine cmd = parser.parse(options, args, false);

    final String host = cmd.getOptionValue("host", "localhost");
    final int port = Integer.parseInt(cmd.getOptionValue("port", "31337"));

    final BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
    final Location defaultLocation = Location.forGrpcInsecure(host, port);
    final FlightClient client = FlightClient.builder(allocator, defaultLocation).build();

    final String inputPath = cmd.getOptionValue("j");

    // 1. Read data from JSON and upload to server.
    FlightDescriptor descriptor = FlightDescriptor.path(inputPath);
    VectorSchemaRoot jsonRoot;
    try (JsonFileReader reader = new JsonFileReader(new File(inputPath), allocator);
        VectorSchemaRoot root = VectorSchemaRoot.create(reader.start(), allocator)) {
      jsonRoot = VectorSchemaRoot.create(root.getSchema(), allocator);
      VectorUnloader unloader = new VectorUnloader(root);
      VectorLoader jsonLoader = new VectorLoader(jsonRoot);
      final CompletableFuture<Void> completed = new CompletableFuture<>();
      FlightClient.ClientStreamListener stream = client.startPut(descriptor, root, reader,
          new StreamListener<PutResult>() {
            int counter = 0;

            @Override
            public void onNext(PutResult val) {
              final String metadata = StandardCharsets.UTF_8.decode(val.getApplicationMetadata().nioBuffer())
                  .toString();
              if (!Integer.toString(counter).equals(metadata)) {
                throw new RuntimeException(
                    String.format("Invalid ACK from server. Expected '%d' but got '%s'.", counter, metadata));
              }
              counter++;
            }

            @Override
            public void onError(Throwable t) {
              completed.completeExceptionally(t);
            }

            @Override
            public void onCompleted() {
              completed.complete(null);
            }
          });
      int counter = 0;
      while (reader.read(root)) {
        final ArrowBuf metadata = allocator.buffer(0);
        metadata.writeBytes(Integer.toString(counter).getBytes(StandardCharsets.UTF_8));
        // Transfers ownership of the buffer, so do not release it ourselves
        stream.putNext(metadata);
        jsonLoader.load(unloader.getRecordBatch());
        root.clear();
        counter++;
      }
      stream.completed();
      // Need to call this, or exceptions from the server get swallowed
      stream.getResult();
      completed.get(30, TimeUnit.SECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new RuntimeException(e);
    }

    // 2. Get the ticket for the data.
    FlightInfo info = client.getInfo(descriptor);
    List<FlightEndpoint> endpoints = info.getEndpoints();
    if (endpoints.isEmpty()) {
      throw new RuntimeException("No endpoints returned from Flight server.");
    }

    for (FlightEndpoint endpoint : info.getEndpoints()) {
      // 3. Download the data from the server.
      List<Location> locations = endpoint.getLocations();
      if (locations.size() == 0) {
        locations = Collections.singletonList(defaultLocation);
      }
      for (Location location : locations) {
        System.out.println("Verifying location " + location.getUri());
        FlightClient readClient = FlightClient.builder(allocator, location).build();
        FlightStream stream = readClient.getStream(endpoint.getTicket());
        VectorSchemaRoot downloadedRoot;
        try (VectorSchemaRoot root = stream.getRoot()) {
          downloadedRoot = VectorSchemaRoot.create(root.getSchema(), allocator);
          VectorLoader loader = new VectorLoader(downloadedRoot);
          VectorUnloader unloader = new VectorUnloader(root);
          while (stream.next()) {
            loader.load(unloader.getRecordBatch());
          }
        }

        // 4. Validate the data.
        Validator.compareVectorSchemaRoot(jsonRoot, downloadedRoot);
      }
    }
  }
}
