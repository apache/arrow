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

package org.apache.arrow.flight.integration.tests;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;

/** Test DoGet with expiration times. */
final class ExpirationTimeDoGetScenario implements Scenario {
  @Override
  public FlightProducer producer(BufferAllocator allocator, Location location) throws Exception {
    return new ExpirationTimeProducer(allocator);
  }

  @Override
  public void buildServer(FlightServer.Builder builder) {
  }

  @Override
  public void client(BufferAllocator allocator, Location location, FlightClient client) throws Exception {
    FlightInfo info = client.getInfo(FlightDescriptor.command("expiration_time".getBytes(StandardCharsets.UTF_8)));

    List<ArrowRecordBatch> batches = new ArrayList<>();

    try {
      for (FlightEndpoint endpoint : info.getEndpoints()) {
        if (batches.size() == 0) {
          IntegrationAssertions.assertFalse("endpoints[0] must not have expiration time",
              endpoint.getExpirationTime().isPresent());
        } else {
          IntegrationAssertions.assertTrue("endpoints[" + batches.size() + "] must have expiration time",
              endpoint.getExpirationTime().isPresent());
        }
        try (FlightStream stream = client.getStream(endpoint.getTicket())) {
          while (stream.next()) {
            batches.add(new VectorUnloader(stream.getRoot()).getRecordBatch());
          }
        }
      }

      // Check data
      IntegrationAssertions.assertEquals(3, batches.size());
      try (final VectorSchemaRoot root = VectorSchemaRoot.create(ExpirationTimeProducer.SCHEMA, allocator)) {
        final VectorLoader loader = new VectorLoader(root);

        loader.load(batches.get(0));
        IntegrationAssertions.assertEquals(1, root.getRowCount());
        IntegrationAssertions.assertEquals(0, ((UInt4Vector) root.getVector(0)).getObject(0));

        loader.load(batches.get(1));
        IntegrationAssertions.assertEquals(1, root.getRowCount());
        IntegrationAssertions.assertEquals(1, ((UInt4Vector) root.getVector(0)).getObject(0));

        loader.load(batches.get(2));
        IntegrationAssertions.assertEquals(1, root.getRowCount());
        IntegrationAssertions.assertEquals(2, ((UInt4Vector) root.getVector(0)).getObject(0));
      }
    } finally {
      AutoCloseables.close(batches);
    }
  }
}
