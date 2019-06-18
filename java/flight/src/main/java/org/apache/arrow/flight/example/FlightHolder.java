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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.types.pojo.Schema;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

/**
 * A logical collection of streams sharing the same schema.
 */
public class FlightHolder implements AutoCloseable {

  private final BufferAllocator allocator;
  private final FlightDescriptor descriptor;
  private final Schema schema;
  private final List<Stream> streams = new CopyOnWriteArrayList<>();

  /**
   * Creates a new instance.
   *
   * @param allocator The allocator to use for allocating buffers to store data.
   * @param descriptor The descriptor for the streams.
   * @param schema  The schema for the stream.
   */
  public FlightHolder(BufferAllocator allocator, FlightDescriptor descriptor, Schema schema) {
    Preconditions.checkArgument(!descriptor.isCommand());
    this.allocator = allocator.newChildAllocator(descriptor.toString(), 0, Long.MAX_VALUE);
    this.descriptor = descriptor;
    this.schema = schema;
  }

  /**
   * Returns the stream based on the ordinal of ExampleTicket.
   */
  public Stream getStream(ExampleTicket ticket) {
    Preconditions.checkArgument(ticket.getOrdinal() < streams.size(), "Unknown stream.");
    Stream stream = streams.get(ticket.getOrdinal());
    stream.verify(ticket);
    return stream;
  }

  /**
   * Adds a new streams which clients can populate via the returned object.
   */
  public Stream.StreamCreator addStream(Schema schema) {
    Preconditions.checkArgument(schema.equals(schema), "Stream schema inconsistent with existing schema.");
    return new Stream.StreamCreator(schema, allocator, t -> {
      synchronized (streams) {
        streams.add(t);
      }
    });
  }

  /**
   * List all available streams as being available at <code>l</code>.
   */
  public FlightInfo getFlightInfo(final Location l) {
    final long bytes = allocator.getAllocatedMemory();
    final long records = streams.stream().collect(Collectors.summingLong(t -> t.getRecordCount()));

    final List<FlightEndpoint> endpoints = new ArrayList<>();
    int i = 0;
    for (Stream s : streams) {
      endpoints.add(
          new FlightEndpoint(
              new ExampleTicket(descriptor.getPath(), i, s.getUuid())
                .toTicket(),
                l));
      i++;
    }
    return new FlightInfo(schema,  descriptor, endpoints, bytes, records);
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(Iterables.concat(streams, ImmutableList.of(allocator)));
  }
}
