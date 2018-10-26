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

import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.ActionType;
import org.apache.arrow.flight.Criteria;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.example.Stream.StreamCreator;
import org.apache.arrow.flight.impl.Flight.PutResult;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * A FlightProducer that hosts an in memory store of Arrow buffers.
 */
public class InMemoryStore implements FlightProducer, AutoCloseable {

  private final ConcurrentMap<FlightDescriptor, FlightHolder> holders = new ConcurrentHashMap<>();
  private final BufferAllocator allocator;
  private final Location location;

  public InMemoryStore(BufferAllocator allocator, Location location) {
    super();
    this.allocator = allocator;
    this.location = location;
  }

  @Override
  public void getStream(Ticket ticket, ServerStreamListener listener) {
    getStream(ticket).sendTo(allocator, listener);
  }

  public Stream getStream(Ticket t) {
    ExampleTicket example = ExampleTicket.from(t);
    FlightDescriptor d = FlightDescriptor.path(example.getPath());
    FlightHolder h = holders.get(d);
    if (h == null) {
      throw new IllegalStateException("Unknown ticket.");
    }

    return h.getStream(example);
  }

  public StreamCreator putStream(final FlightDescriptor descriptor, final Schema schema) {
    final FlightHolder h = holders.computeIfAbsent(
        descriptor,
        t -> new FlightHolder(allocator, t, schema));

    return h.addStream(schema);
  }

  @Override
  public void listFlights(Criteria criteria, StreamListener<FlightInfo> listener) {
    try {
      for (FlightHolder h : holders.values()) {
        listener.onNext(h.getFlightInfo(location));
      }
      listener.onCompleted();
    } catch (Exception ex) {
      listener.onError(ex);
    }
  }

  @Override
  public FlightInfo getFlightInfo(FlightDescriptor descriptor) {
    FlightHolder h = holders.get(descriptor);
    if (h == null) {
      throw new IllegalStateException("Unknown descriptor.");
    }

    return h.getFlightInfo(location);
  }

  @Override
  public Callable<PutResult> acceptPut(final FlightStream flightStream) {
    return () -> {
      StreamCreator creator = null;
      boolean success = false;
      try (VectorSchemaRoot root = flightStream.getRoot()) {
        final FlightHolder h = holders.computeIfAbsent(
            flightStream.getDescriptor(),
            t -> new FlightHolder(allocator, t, flightStream.getSchema()));

        creator = h.addStream(flightStream.getSchema());

        VectorUnloader unloader = new VectorUnloader(root);
        while (flightStream.next()) {
          creator.add(unloader.getRecordBatch());
        }
        creator.complete();
        success = true;
        return PutResult.getDefaultInstance();
      } finally {
        if (!success) {
          creator.drop();
        }
      }

    };

  }

  @Override
  public Result doAction(Action action) {
    switch (action.getType()) {
      case "drop":
        return new Result(new byte[0]);
        // not implemented.
      default:
        throw new UnsupportedOperationException();
    }
  }

  @Override
  public void listActions(StreamListener<ActionType> listener) {
    listener.onNext(new ActionType("get", "pull a stream. Action must be done via standard get mechanism"));
    listener.onNext(new ActionType("put", "push a stream. Action must be done via standard get mechanism"));
    listener.onNext(new ActionType("drop", "delete a flight. Action body is a JSON encoded path."));
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(holders.values());
    holders.clear();
  }

}
