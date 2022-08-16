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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.NoOpFlightProducer;
import org.apache.arrow.flight.PutResult;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.DictionaryUtility;

/**
 * A FlightProducer that hosts an in memory store of Arrow buffers. Used for integration testing.
 */
public class IntegrationProducer extends NoOpFlightProducer implements AutoCloseable {
  private final ConcurrentMap<FlightDescriptor, Dataset> datasets = new ConcurrentHashMap<>();
  private final BufferAllocator allocator;
  private Location location;

  /**
   * Constructs a new instance.
   *
   * @param allocator The allocator for creating new Arrow buffers.
   * @param location The location of the storage.
   */
  public IntegrationProducer(BufferAllocator allocator, Location location) {
    super();
    this.allocator = allocator;
    this.location = location;
  }

  /**
   * Update the location after server start.
   *
   * <p>Useful for binding to port 0 to get a free port.
   */
  public void setLocation(Location location) {
    this.location = location;
  }

  @Override
  public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
    try {
      FlightDescriptor descriptor = FlightDescriptor.deserialize(ByteBuffer.wrap(ticket.getBytes()));
      Dataset dataset = datasets.get(descriptor);
      if (dataset == null) {
        listener.error(CallStatus.NOT_FOUND.withDescription("Unknown ticket: " + descriptor).toRuntimeException());
        return;
      }
      dataset.streamTo(allocator, listener);
    } catch (Exception ex) {
      listener.error(IntegrationAssertions.toFlightRuntimeException(ex));
    }
  }

  @Override
  public FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor) {
    Dataset h = datasets.get(descriptor);
    if (h == null) {
      throw CallStatus.NOT_FOUND.withDescription("Unknown descriptor: " + descriptor).toRuntimeException();
    }
    return h.getFlightInfo(location);
  }

  @Override
  public Runnable acceptPut(CallContext context,
      final FlightStream flightStream, final StreamListener<PutResult> ackStream) {
    return () -> {
      List<ArrowRecordBatch> batches = new ArrayList<>();
      try {
        try (VectorSchemaRoot root = flightStream.getRoot()) {
          VectorUnloader unloader = new VectorUnloader(root);
          while (flightStream.next()) {
            ackStream.onNext(PutResult.metadata(flightStream.getLatestMetadata()));
            batches.add(unloader.getRecordBatch());
          }
          // Closing the stream will release the dictionaries, take ownership
          final Dataset dataset = new Dataset(
                  flightStream.getDescriptor(),
                  flightStream.getSchema(),
                  flightStream.takeDictionaryOwnership(),
                  batches);
          batches.clear();
          datasets.put(flightStream.getDescriptor(), dataset);
        } finally {
          AutoCloseables.close(batches);
        }
      } catch (Exception ex) {
        ackStream.onError(IntegrationAssertions.toFlightRuntimeException(ex));
      }
    };
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(datasets.values());
    datasets.clear();
  }

  private static final class Dataset implements AutoCloseable {
    private final FlightDescriptor descriptor;
    private final Schema schema;
    private final DictionaryProvider dictionaryProvider;
    private final List<ArrowRecordBatch> batches;

    private Dataset(FlightDescriptor descriptor,
                    Schema schema,
                    DictionaryProvider dictionaryProvider,
                    List<ArrowRecordBatch> batches) {
      this.descriptor = descriptor;
      this.schema = schema;
      this.dictionaryProvider = dictionaryProvider;
      this.batches = new ArrayList<>(batches);
    }

    public FlightInfo getFlightInfo(Location location) {
      ByteBuffer serializedDescriptor = descriptor.serialize();
      byte[] descriptorBytes = new byte[serializedDescriptor.remaining()];
      serializedDescriptor.get(descriptorBytes);
      final List<FlightEndpoint> endpoints = Collections.singletonList(
              new FlightEndpoint(new Ticket(descriptorBytes), location));
      return new FlightInfo(
              messageFormatSchema(),
              descriptor,
              endpoints,
              batches.stream().mapToLong(ArrowRecordBatch::computeBodyLength).sum(),
              batches.stream().mapToInt(ArrowRecordBatch::getLength).sum());
    }

    private Schema messageFormatSchema() {
      final Set<Long> dictionaryIdsUsed = new HashSet<>();
      final List<Field> messageFormatFields = schema.getFields()
              .stream()
              .map(f -> DictionaryUtility.toMessageFormat(f, dictionaryProvider, dictionaryIdsUsed))
              .collect(Collectors.toList());
      return new Schema(messageFormatFields, schema.getCustomMetadata());
    }

    @Override
    public void close() throws Exception {
      AutoCloseables.close(batches);
    }

    public void streamTo(BufferAllocator allocator, ServerStreamListener listener) {
      try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
        listener.start(root, dictionaryProvider);
        final VectorLoader loader = new VectorLoader(root);
        int counter = 0;
        for (ArrowRecordBatch batch : batches) {
          final byte[] rawMetadata = Integer.toString(counter).getBytes(StandardCharsets.UTF_8);
          final ArrowBuf metadata = allocator.buffer(rawMetadata.length);
          metadata.writeBytes(rawMetadata);
          loader.load(batch);
          // Transfers ownership of the buffer - do not free buffer ourselves
          listener.putNext(metadata);
          counter++;
        }
        listener.completed();
      }
    }
  }
}
