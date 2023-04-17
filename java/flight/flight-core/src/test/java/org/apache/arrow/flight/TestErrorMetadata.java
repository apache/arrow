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

import static org.apache.arrow.flight.FlightTestUtil.LOCALHOST;
import static org.apache.arrow.flight.Location.forGrpcInsecure;

import org.apache.arrow.flight.perf.impl.PerfOuterClass;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.rpc.Status;

import io.grpc.Metadata;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.protobuf.StatusProto;

public class TestErrorMetadata {
  private static final Metadata.BinaryMarshaller<Status> marshaller =
          ProtoUtils.metadataMarshaller(Status.getDefaultInstance());

  /** Ensure metadata attached to a gRPC error is propagated. */
  @Test
  public void testGrpcMetadata() throws Exception {
    PerfOuterClass.Perf perf = PerfOuterClass.Perf.newBuilder()
                .setStreamCount(12)
                .setRecordsPerBatch(1000)
                .setRecordsPerStream(1000000L)
                .build();
    StatusRuntimeExceptionProducer producer = new StatusRuntimeExceptionProducer(perf);
    try (final BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
         final FlightServer s = FlightServer.builder(allocator, forGrpcInsecure(LOCALHOST, 0), producer).build()
             .start();
         final FlightClient client = FlightClient.builder(allocator, s.getLocation()).build()) {
      final CallStatus flightStatus = FlightTestUtil.assertCode(FlightStatusCode.CANCELLED, () -> {
        FlightStream stream = client.getStream(new Ticket("abs".getBytes()));
        stream.next();
      });
      PerfOuterClass.Perf newPerf = null;
      ErrorFlightMetadata metadata = flightStatus.metadata();
      Assertions.assertNotNull(metadata);
      Assertions.assertEquals(2, metadata.keys().size());
      Assertions.assertTrue(metadata.containsKey("grpc-status-details-bin"));
      Status status = marshaller.parseBytes(metadata.getByte("grpc-status-details-bin"));
      for (Any details : status.getDetailsList()) {
        if (details.is(PerfOuterClass.Perf.class)) {
          try {
            newPerf = details.unpack(PerfOuterClass.Perf.class);
          } catch (InvalidProtocolBufferException e) {
            Assertions.fail();
          }
        }
      }
      Assertions.assertNotNull(newPerf);
      Assertions.assertEquals(perf, newPerf);
    }
  }

  /** Ensure metadata attached to a Flight error is propagated. */
  @Test
  public void testFlightMetadata() throws Exception {
    try (final BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
         final FlightServer s = FlightServer.builder(allocator, forGrpcInsecure(LOCALHOST, 0), new CallStatusProducer())
             .build().start();
         final FlightClient client = FlightClient.builder(allocator, s.getLocation()).build()) {
      CallStatus flightStatus = FlightTestUtil.assertCode(FlightStatusCode.INVALID_ARGUMENT, () -> {
        FlightStream stream = client.getStream(new Ticket(new byte[0]));
        stream.next();
      });
      ErrorFlightMetadata metadata = flightStatus.metadata();
      Assertions.assertNotNull(metadata);
      Assertions.assertEquals("foo", metadata.get("x-foo"));
      Assertions.assertArrayEquals(new byte[]{1}, metadata.getByte("x-bar-bin"));

      flightStatus = FlightTestUtil.assertCode(FlightStatusCode.INVALID_ARGUMENT, () -> {
        client.getInfo(FlightDescriptor.command(new byte[0]));
      });
      metadata = flightStatus.metadata();
      Assertions.assertNotNull(metadata);
      Assertions.assertEquals("foo", metadata.get("x-foo"));
      Assertions.assertArrayEquals(new byte[]{1}, metadata.getByte("x-bar-bin"));
    }
  }

  private static class StatusRuntimeExceptionProducer extends NoOpFlightProducer {
    private final PerfOuterClass.Perf perf;

    private StatusRuntimeExceptionProducer(PerfOuterClass.Perf perf) {
      this.perf = perf;
    }

    @Override
    public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
      StatusRuntimeException sre = StatusProto.toStatusRuntimeException(Status.newBuilder()
              .setCode(1)
              .setMessage("Testing 1 2 3")
              .addDetails(Any.pack(perf, "arrow/meta/types"))
              .build());
      listener.error(sre);
    }
  }

  private static class CallStatusProducer extends NoOpFlightProducer {
    ErrorFlightMetadata metadata;

    CallStatusProducer() {
      this.metadata = new ErrorFlightMetadata();
      metadata.insert("x-foo", "foo");
      metadata.insert("x-bar-bin", new byte[]{1});
    }

    @Override
    public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
      listener.error(CallStatus.INVALID_ARGUMENT.withDescription("Failed").withMetadata(metadata).toRuntimeException());
    }

    @Override
    public FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor) {
      throw CallStatus.INVALID_ARGUMENT.withDescription("Failed").withMetadata(metadata).toRuntimeException();
    }
  }
}
