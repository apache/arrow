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

package org.apache.arrow.flight.perf;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.ActionType;
import org.apache.arrow.flight.Criteria;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.auth.ServerAuthHandler;
import org.apache.arrow.flight.example.ExampleFlightServer;
import org.apache.arrow.flight.impl.Flight.PutResult;
import org.apache.arrow.flight.perf.impl.PerfOuterClass.Perf;
import org.apache.arrow.flight.perf.impl.PerfOuterClass.Token;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.InvalidProtocolBufferException;

public class PerformanceTestServer implements AutoCloseable {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ExampleFlightServer.class);

  private final FlightServer flightServer;
  private final Location location;
  private final BufferAllocator allocator;
  private final PerfProducer producer;

  public PerformanceTestServer(BufferAllocator incomingAllocator, Location location) {
    this.allocator = incomingAllocator.newChildAllocator("perf-server", 0, Long.MAX_VALUE);
    this.location = location;
    this.producer = new PerfProducer();
    this.flightServer = new FlightServer(this.allocator, location.getPort(), producer, ServerAuthHandler.NO_OP);
  }

  public Location getLocation() {
    return location;
  }

  public void start() throws IOException {
    flightServer.start();
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(flightServer, allocator);
  }

  private final class PerfProducer implements FlightProducer {

    @Override
    public void getStream(Ticket ticket, ServerStreamListener listener) {
      VectorSchemaRoot root = null;
      try {
        Token token = Token.parseFrom(ticket.getBytes());
        Perf perf = token.getDefinition();
        Schema schema = Schema.deserialize(ByteBuffer.wrap(perf.getSchema().toByteArray()));
        root = VectorSchemaRoot.create(schema, allocator);
        BigIntVector a = (BigIntVector) root.getVector("a");
        BigIntVector b = (BigIntVector) root.getVector("b");
        BigIntVector c = (BigIntVector) root.getVector("c");
        BigIntVector d = (BigIntVector) root.getVector("d");
        listener.start(root);
        root.allocateNew();

        int current = 0;
        long i = token.getStart();
        while (i < token.getEnd()) {
          if (listener.isCancelled()) {
            root.clear();
            return;
          }

          if (TestPerf.VALIDATE) {
            a.setSafe(current, i);
          }

          i++;
          current++;
          if (i % perf.getRecordsPerBatch() == 0) {
            root.setRowCount(current);

            while (!listener.isReady()) {
              //Thread.sleep(0, nanos);
            }

            if (listener.isCancelled()) {
              root.clear();
              return;
            }
            listener.putNext();
            current = 0;
            root.allocateNew();
          }
        }

        // send last partial batch.
        if (current != 0) {
          root.setRowCount(current);
          listener.putNext();
        }
        listener.completed();
      } catch (InvalidProtocolBufferException e) {
        throw new RuntimeException(e);
      } finally {
        try {
          AutoCloseables.close(root);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }

    @Override
    public void listFlights(Criteria criteria, StreamListener<FlightInfo> listener) {
    }

    @Override
    public FlightInfo getFlightInfo(FlightDescriptor descriptor) {
      try {
        Preconditions.checkArgument(descriptor.isCommand());
        Perf exec = Perf.parseFrom(descriptor.getCommand());

        final Schema pojoSchema = new Schema(ImmutableList.of(
            Field.nullable("a", MinorType.BIGINT.getType()),
            Field.nullable("b", MinorType.BIGINT.getType()),
            Field.nullable("c", MinorType.BIGINT.getType()),
            Field.nullable("d", MinorType.BIGINT.getType())
            ));

        Token token = Token.newBuilder().setDefinition(exec)
            .setStart(0)
            .setEnd(exec.getRecordsPerStream())
            .build();
        final Ticket ticket = new Ticket(token.toByteArray());

        List<FlightEndpoint> endpoints = new ArrayList<>();
        for (int i = 0; i < exec.getStreamCount(); i++) {
          endpoints.add(new FlightEndpoint(ticket, getLocation()));
        }

        return new FlightInfo(pojoSchema, descriptor, endpoints, -1,
            exec.getRecordsPerStream() * exec.getStreamCount());
      } catch (InvalidProtocolBufferException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public Callable<PutResult> acceptPut(FlightStream flightStream) {
      return null;
    }

    @Override
    public Result doAction(Action action) {
      return null;
    }

    @Override
    public void listActions(StreamListener<ActionType> listener) {
    }

  }
}



