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

import java.io.File;

import org.apache.arrow.flight.TestBasicOperation.Producer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestServerOptions {

  @Test
  public void domainSocket() throws Exception {
    Assume.assumeTrue("We have a native transport available", FlightTestUtil.isNativeTransportAvailable());
    final File domainSocket = File.createTempFile("flight-unit-test-", ".sock");
    Assert.assertTrue(domainSocket.delete());
    final Location location = Location.forGrpcDomainSocket(domainSocket.getAbsolutePath());
    try (
        BufferAllocator a = new RootAllocator(Long.MAX_VALUE);
        Producer producer = new Producer(a);
        FlightServer s =
            FlightTestUtil.getStartedServer(
                (port) -> FlightServer.builder(a, location, producer).build()
            )) {
      try (FlightClient c = FlightClient.builder(a, location).build()) {
        FlightStream stream = c.getStream(new Ticket(new byte[0]));
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
