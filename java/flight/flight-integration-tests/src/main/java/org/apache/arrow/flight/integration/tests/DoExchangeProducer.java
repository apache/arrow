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

import java.util.Arrays;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.NoOpFlightProducer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;

/** The server used for testing the Flight do_exchange method. */
final class DoExchangeProducer extends NoOpFlightProducer {
  private final BufferAllocator allocator;

  DoExchangeProducer(BufferAllocator allocator) {
    this.allocator = allocator;
  }

  @Override
  public void doExchange(CallContext context, FlightStream reader, ServerStreamListener writer) {
    FlightDescriptor descriptor = reader.getDescriptor();
    if (descriptor.isCommand()) {
      if (Arrays.equals(DoExchangeEchoScenario.COMMAND, descriptor.getCommand())) {
        doEcho(reader, writer);
      }
    }
    throw CallStatus.UNIMPLEMENTED
        .withDescription("Unsupported descriptor: " + descriptor.toString())
        .toRuntimeException();
  }

  private void doEcho(FlightStream reader, ServerStreamListener writer) {
    VectorSchemaRoot root = null;
    VectorLoader loader = null;
    while (reader.next()) {
      if (reader.hasRoot()) {
        if (root == null) {
          root = VectorSchemaRoot.create(reader.getSchema(), allocator);
          loader = new VectorLoader(root);
          writer.start(root);
        }
        VectorUnloader unloader = new VectorUnloader(reader.getRoot());
        try (final ArrowRecordBatch arb = unloader.getRecordBatch()) {
          loader.load(arb);
        }
        if (reader.getLatestMetadata() != null) {
          reader.getLatestMetadata().getReferenceManager().retain();
          writer.putNext(reader.getLatestMetadata());
        } else {
          writer.putNext();
        }
      } else {
        // Pure metadata
        reader.getLatestMetadata().getReferenceManager().retain();
        writer.putMetadata(reader.getLatestMetadata());
      }
    }
    if (root != null) {
      root.close();
    }
    writer.completed();
  }
}
