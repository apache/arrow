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

import org.apache.arrow.flight.FlightClient.ClientStreamListener;
import org.apache.arrow.flight.TestBasicOperation.Producer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Assert;
import org.junit.Test;

public class TestFlightClient {
  /**
   * ARROW-5063: make sure two clients to the same location can be closed independently.
   */
  @Test
  public void independentShutdown() throws Exception {
    try (final BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
        final FlightServer server = FlightTestUtil.getStartedServer(
            location -> FlightServer.builder(allocator, location,
                new Producer(allocator)).build())) {
      final Location location = Location.forGrpcInsecure(FlightTestUtil.LOCALHOST, server.getPort());
      final Schema schema = new Schema(Collections.singletonList(Field.nullable("a", new ArrowType.Int(32, true))));
      try (final FlightClient client1 = FlightClient.builder(allocator, location).build();
          final VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
        // Use startPut as this ensures the RPC won't finish until we want it to
        final ClientStreamListener listener = client1.startPut(FlightDescriptor.path("test"), root,
            new AsyncPutListener());
        try (final FlightClient client2 = FlightClient.builder(allocator, location).build()) {
          client2.listActions().forEach(actionType -> Assert.assertNotNull(actionType.getType()));
        }
        listener.completed();
        listener.getResult();
      }
    }
  }
}
