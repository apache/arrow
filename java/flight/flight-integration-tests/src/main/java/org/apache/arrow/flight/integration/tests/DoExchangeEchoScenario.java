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
import java.util.Collections;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Validator;

/** Test DoExchange by echoing data back to the client. */
final class DoExchangeEchoScenario implements Scenario {
  public static final byte[] COMMAND = "echo".getBytes(StandardCharsets.UTF_8);

  @Override
  public FlightProducer producer(BufferAllocator allocator, Location location) throws Exception {
    return new DoExchangeProducer(allocator);
  }

  @Override
  public void buildServer(FlightServer.Builder builder) {}

  @Override
  public void client(BufferAllocator allocator, Location location, FlightClient client)
      throws Exception {
    final Schema schema =
        new Schema(Collections.singletonList(Field.notNullable("x", new ArrowType.Int(32, true))));
    try (final FlightClient.ExchangeReaderWriter stream =
            client.doExchange(FlightDescriptor.command(COMMAND));
        final VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      final FlightStream reader = stream.getReader();

      // Write data and check that it gets echoed back.
      IntVector iv = (IntVector) root.getVector("x");
      iv.allocateNew();
      stream.getWriter().start(root);
      int rowCount = 10;
      for (int batchIdx = 0; batchIdx < 4; batchIdx++) {
        for (int rowIdx = 0; rowIdx < rowCount; rowIdx++) {
          iv.setSafe(rowIdx, batchIdx + rowIdx);
        }
        root.setRowCount(rowCount);
        boolean writeMetadata = batchIdx % 2 == 0;
        final byte[] rawMetadata = Integer.toString(batchIdx).getBytes(StandardCharsets.UTF_8);
        if (writeMetadata) {
          final ArrowBuf metadata = allocator.buffer(rawMetadata.length);
          metadata.writeBytes(rawMetadata);
          stream.getWriter().putNext(metadata);
        } else {
          stream.getWriter().putNext();
        }

        IntegrationAssertions.assertTrue("Unexpected end of reader", reader.next());
        if (writeMetadata) {
          IntegrationAssertions.assertNotNull(reader.getLatestMetadata());
          final byte[] readMetadata = new byte[rawMetadata.length];
          reader.getLatestMetadata().readBytes(readMetadata);
          IntegrationAssertions.assertEquals(rawMetadata, readMetadata);
        } else {
          IntegrationAssertions.assertNull(reader.getLatestMetadata());
        }
        IntegrationAssertions.assertEquals(root.getSchema(), reader.getSchema());
        Validator.compareVectorSchemaRoot(reader.getRoot(), root);
      }

      stream.getWriter().completed();
      IntegrationAssertions.assertFalse("Expected to reach end of reader", reader.next());
    }
  }
}
