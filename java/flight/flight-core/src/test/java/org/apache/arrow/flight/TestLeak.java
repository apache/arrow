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

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

/**
 * Tests for scenarios where Flight could leak memory.
 */
public class TestLeak {

  private static final int ROWS = 2048;

  private static Schema getSchema() {
    return new Schema(Arrays.asList(
        Field.nullable("0", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
        Field.nullable("1", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
        Field.nullable("2", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
        Field.nullable("3", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
        Field.nullable("4", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
        Field.nullable("5", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
        Field.nullable("6", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
        Field.nullable("7", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
        Field.nullable("8", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
        Field.nullable("9", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
        Field.nullable("10", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE))
    ));
  }

  /**
   * Ensure that if the client cancels, the server does not leak memory.
   *
   * <p>In gRPC, canceling the stream from the client sends an event to the server. Once processed, gRPC will start
   * silently rejecting messages sent by the server. However, Flight depends on gRPC processing these messages in order
   * to free the associated memory.
   */
  @Test
  public void testCancelingDoGetDoesNotLeak() throws Exception {
    final CountDownLatch callFinished = new CountDownLatch(1);
    try (final BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
         final FlightServer s = FlightServer.builder(allocator, forGrpcInsecure(LOCALHOST, 0),
                 new LeakFlightProducer(allocator, callFinished))
             .build().start();
         final FlightClient client = FlightClient.builder(allocator, s.getLocation()).build()) {

      final FlightStream stream = client.getStream(new Ticket(new byte[0]));
      stream.getRoot();
      stream.cancel("Cancel", null);

      // Wait for the call to finish. (Closing the allocator while a call is ongoing is a guaranteed leak.)
      callFinished.await(60, TimeUnit.SECONDS);

      s.shutdown();
      s.awaitTermination();
    }
  }

  @Test
  public void testCancelingDoPutDoesNotBlock() throws Exception {
    final CountDownLatch callFinished = new CountDownLatch(1);
    try (final BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
         final FlightServer s = FlightServer.builder(allocator, forGrpcInsecure(LOCALHOST, 0),
                 new LeakFlightProducer(allocator, callFinished))
             .build().start();
         final FlightClient client = FlightClient.builder(allocator, s.getLocation()).build()) {

      try (final VectorSchemaRoot root = VectorSchemaRoot.create(getSchema(), allocator)) {
        final FlightDescriptor descriptor = FlightDescriptor.command(new byte[0]);
        final SyncPutListener listener = new SyncPutListener();
        final FlightClient.ClientStreamListener stream = client.startPut(descriptor, root, listener);
        // Wait for the server to cancel
        callFinished.await(60, TimeUnit.SECONDS);

        for (int col = 0; col < 11; col++) {
          final Float8Vector vector = (Float8Vector) root.getVector(Integer.toString(col));
          vector.allocateNew();
          for (int row = 0; row < ROWS; row++) {
            vector.setSafe(row, 10.);
          }
        }
        root.setRowCount(ROWS);
        // Unlike DoGet, this method fairly reliably will write the message to the stream, so even without the fix
        // for ARROW-7343, this won't leak memory.
        // However, it will block if FlightClient doesn't check for cancellation.
        stream.putNext();
        stream.completed();
      }

      s.shutdown();
      s.awaitTermination();
    }
  }

  /**
   * A FlightProducer that always produces a fixed data stream with metadata on the side.
   */
  private static class LeakFlightProducer extends NoOpFlightProducer {

    private final BufferAllocator allocator;
    private final CountDownLatch callFinished;

    public LeakFlightProducer(BufferAllocator allocator, CountDownLatch callFinished) {
      this.allocator = allocator;
      this.callFinished = callFinished;
    }

    @Override
    public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
      BufferAllocator childAllocator = allocator.newChildAllocator("foo", 0, Long.MAX_VALUE);
      VectorSchemaRoot root = VectorSchemaRoot.create(TestLeak.getSchema(), childAllocator);
      root.allocateNew();
      listener.start(root);

      // We can't poll listener#isCancelled since gRPC has two distinct "is cancelled" flags.
      // TODO: should we continue leaking gRPC semantics? Can we even avoid this?
      listener.setOnCancelHandler(() -> {
        try {
          for (int col = 0; col < 11; col++) {
            final Float8Vector vector = (Float8Vector) root.getVector(Integer.toString(col));
            vector.allocateNew();
            for (int row = 0; row < ROWS; row++) {
              vector.setSafe(row, 10.);
            }
          }
          root.setRowCount(ROWS);
          // Once the call is "really cancelled" (setOnCancelListener has run/is running), this call is actually a
          // no-op on the gRPC side and will leak the ArrowMessage unless Flight checks for this.
          listener.putNext();
          listener.completed();
        } finally {
          try {
            root.close();
            childAllocator.close();
          } finally {
            // Don't let the test hang if we throw above
            callFinished.countDown();
          }
        }
      });
    }

    @Override
    public Runnable acceptPut(CallContext context,
        FlightStream flightStream, StreamListener<PutResult> ackStream) {
      return () -> {
        flightStream.getRoot();
        ackStream.onError(CallStatus.CANCELLED.withDescription("CANCELLED").toRuntimeException());
        callFinished.countDown();
        ackStream.onCompleted();
      };
    }
  }
}
