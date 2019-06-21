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

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightClient.ClientStreamListener;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.FlightTestUtil;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Ensure that example server supports get and put.
 */
@org.junit.Ignore
public class TestExampleServer {

  private BufferAllocator allocator;
  private BufferAllocator caseAllocator;
  private ExampleFlightServer server;
  private FlightClient client;

  @Before
  public void start() throws IOException {
    allocator = new RootAllocator(Long.MAX_VALUE);

    Location l = Location.forGrpcInsecure(FlightTestUtil.LOCALHOST, 12233);
    if (!Boolean.getBoolean("disableServer")) {
      System.out.println("Starting server.");
      server = new ExampleFlightServer(allocator, l);
      server.start();
    } else {
      System.out.println("Skipping server startup.");
    }
    client = FlightClient.builder(allocator, l).build();
    caseAllocator = allocator.newChildAllocator("test-case", 0, Long.MAX_VALUE);
  }

  @After
  public void after() throws Exception {
    AutoCloseables.close(server, client, caseAllocator, allocator);
  }

  @Test
  public void putStream() {
    BufferAllocator a = caseAllocator;
    final int size = 10;

    IntVector iv = new IntVector("c1", a);

    VectorSchemaRoot root = VectorSchemaRoot.of(iv);
    ClientStreamListener listener = client.startPut(FlightDescriptor.path("hello"), root);

    //batch 1
    root.allocateNew();
    for (int i = 0; i < size; i++) {
      iv.set(i, i);
    }
    iv.setValueCount(size);
    root.setRowCount(size);
    listener.putNext();

    // batch 2

    root.allocateNew();
    for (int i = 0; i < size; i++) {
      iv.set(i, i + size);
    }
    iv.setValueCount(size);
    root.setRowCount(size);
    listener.putNext();
    root.clear();
    listener.completed();

    // wait for ack to avoid memory leaks.
    listener.getResult();

    FlightInfo info = client.getInfo(FlightDescriptor.path("hello"));
    FlightStream stream = client.getStream(info.getEndpoints().get(0).getTicket());
    VectorSchemaRoot newRoot = stream.getRoot();
    while (stream.next()) {
      newRoot.clear();
    }
  }
}
