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

import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.example.ExampleFlightServer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

class IntegrationTestServer {
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(IntegrationTestServer.class);
  private final Options options;

  private IntegrationTestServer() {
    options = new Options();
    options.addOption("port", true, "The port to serve on.");
  }

  private void run(String[] args) throws Exception {
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = parser.parse(options, args, false);
    final int port = Integer.parseInt(cmd.getOptionValue("port", "31337"));

    final BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
    final ExampleFlightServer efs = new ExampleFlightServer(allocator, new Location("localhost", port));
    efs.start();
    // Print out message for integration test script
    System.out.println("Server listening on localhost:" + port);

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        System.out.println("\nExiting...");
        AutoCloseables.close(efs, allocator);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }));

    efs.awaitTermination();
  }

  public static void main(String[] args) {
    try {
      new IntegrationTestServer().run(args);
    } catch (ParseException e) {
      fatalError("Error parsing arguments", e);
    } catch (Exception e) {
      fatalError("Runtime error", e);
    }
  }

  private static void fatalError(String message, Throwable e) {
    System.err.println(message);
    System.err.println(e.getMessage());
    LOGGER.error(message, e);
    System.exit(1);
  }

}
