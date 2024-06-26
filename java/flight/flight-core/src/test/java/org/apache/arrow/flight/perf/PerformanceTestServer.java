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

import com.google.common.collect.ImmutableList;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.arrow.flight.BackpressureStrategy;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.NoOpFlightProducer;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.perf.impl.PerfOuterClass.Perf;
import org.apache.arrow.flight.perf.impl.PerfOuterClass.Token;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

public class PerformanceTestServer implements AutoCloseable {

  private final FlightServer flightServer;
  private final BufferAllocator allocator;
  private final PerfProducer producer;
  private final boolean isNonBlocking;

  public PerformanceTestServer(BufferAllocator incomingAllocator, Location location) {
    this(
        incomingAllocator,
        location,
        new BackpressureStrategy() {
          private FlightProducer.ServerStreamListener listener;

          @Override
          public void register(FlightProducer.ServerStreamListener listener) {
            this.listener = listener;
          }

          @Override
          public WaitResult waitForListener(long timeout) {
            while (!listener.isReady() && !listener.isCancelled()) {
              // busy wait
            }
            return WaitResult.READY;
          }
        },
        false);
  }

  public PerformanceTestServer(
      BufferAllocator incomingAllocator,
      Location location,
      BackpressureStrategy bpStrategy,
      boolean isNonBlocking) {
    this.allocator = incomingAllocator.newChildAllocator("perf-server", 0, Long.MAX_VALUE);
    this.producer = new PerfProducer(bpStrategy);
    this.flightServer = FlightServer.builder(this.allocator, location, producer).build();
    this.isNonBlocking = isNonBlocking;
  }

  public Location getLocation() {
    return flightServer.getLocation();
  }

  public PerformanceTestServer start() throws IOException {
    flightServer.start();
    return this;
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(flightServer, allocator);
  }

  private final class PerfProducer extends NoOpFlightProducer {
    private final BackpressureStrategy bpStrategy;

    private PerfProducer(BackpressureStrategy bpStrategy) {
      this.bpStrategy = bpStrategy;
    }

    @Override
    public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
      bpStrategy.register(listener);
      final Runnable loadData =
          () -> {
            Token token = null;
            try {
              token = Token.parseFrom(ticket.getBytes());
            } catch (InvalidProtocolBufferException e) {
              throw new RuntimeException(e);
            }
            Perf perf = token.getDefinition();
            Schema schema = Schema.deserializeMessage(perf.getSchema().asReadOnlyByteBuffer());
            try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
                BigIntVector a = (BigIntVector) root.getVector("a")) {
              listener.setUseZeroCopy(true);
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

                  bpStrategy.waitForListener(0);
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
            }
          };

      if (!isNonBlocking) {
        loadData.run();
      } else {
        final ExecutorService service = Executors.newSingleThreadExecutor();
        Future<?> unused = service.submit(loadData);
        service.shutdown();
      }
    }

    @Override
    public FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor) {
      try {
        Preconditions.checkArgument(descriptor.isCommand());
        Perf exec = Perf.parseFrom(descriptor.getCommand());

        final Schema pojoSchema =
            new Schema(
                ImmutableList.of(
                    Field.nullable("a", MinorType.BIGINT.getType()),
                    Field.nullable("b", MinorType.BIGINT.getType()),
                    Field.nullable("c", MinorType.BIGINT.getType()),
                    Field.nullable("d", MinorType.BIGINT.getType())));

        Token token =
            Token.newBuilder()
                .setDefinition(exec)
                .setStart(0)
                .setEnd(exec.getRecordsPerStream())
                .build();
        final Ticket ticket = new Ticket(token.toByteArray());

        List<FlightEndpoint> endpoints = new ArrayList<>();
        for (int i = 0; i < exec.getStreamCount(); i++) {
          endpoints.add(new FlightEndpoint(ticket, getLocation()));
        }

        return new FlightInfo(
            pojoSchema,
            descriptor,
            endpoints,
            -1,
            exec.getRecordsPerStream() * exec.getStreamCount());
      } catch (InvalidProtocolBufferException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
