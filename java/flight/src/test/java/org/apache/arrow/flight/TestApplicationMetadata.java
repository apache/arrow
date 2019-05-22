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

import java.util.Arrays;
import java.util.Collections;

import org.apache.arrow.flight.FlightProducer.StreamListener;
import org.apache.arrow.flight.auth.ServerAuthHandler;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Tests for application-specific metadata support in Flight.
 */
public class TestApplicationMetadata {

  /**
   * Ensure that a client can read the metadata sent from the server.
   */
  @Test
  // This test is consistently flaky on CI, unfortunately.
  @Ignore
  public void retrieveMetadata() {
    try (final BufferAllocator a = new RootAllocator(Long.MAX_VALUE);
        final FlightServer s =
            FlightTestUtil.getStartedServer(
                (location) -> FlightServer.builder(a, location, new MetadataFlightProducer(a)).build());
        final FlightClient client = FlightClient.builder(a, s.getLocation()).build();
        final FlightStream stream = client.getStream(new Ticket(new byte[0]))) {
      byte i = 0;
      while (stream.next()) {
        final IntVector vector = (IntVector) stream.getRoot().getVector("a");
        Assert.assertEquals(1, vector.getValueCount());
        Assert.assertEquals(10, vector.get(0));
        Assert.assertArrayEquals(new byte[]{i}, stream.getLatestMetadata());
        i++;
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Ensure that a client can send metadata to the server.
   */
  @Test
  public void uploadMetadata() {
    final FlightDescriptor descriptor = FlightDescriptor.path("test");
    final Schema schema = new Schema(Collections.singletonList(Field.nullable("a", new ArrowType.Int(32, true))));
    try (final BufferAllocator a = new RootAllocator(Long.MAX_VALUE);
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, a);
        final FlightServer s =
            FlightTestUtil.getStartedServer(
                (location) -> FlightServer.builder(a, location, new MetadataFlightProducer(a)).build());
        final FlightClient client = FlightClient.builder(a, s.getLocation()).build()) {

      final StreamListener<PutResult> listener = new StreamListener<PutResult>() {
        int counter = 0;

        @Override
        public void onNext(PutResult val) {
          Assert.assertNotNull(val);
          Assert.assertEquals(counter, val.getApplicationMetadata().get(0));
          counter++;
        }

        @Override
        public void onError(Throwable t) {
          Assert.fail(t.toString());
        }

        @Override
        public void onCompleted() {
          Assert.assertEquals(10, counter);
        }
      };
      final FlightClient.ClientStreamListener writer = client.startPut(descriptor, root, listener);

      root.allocateNew();
      for (byte i = 0; i < 10; i++) {
        final IntVector vector = (IntVector) root.getVector("a");
        vector.set(0, 10);
        vector.setValueCount(1);
        root.setRowCount(1);
        writer.putNext(new byte[]{i});
      }
      writer.completed();
      // Must attempt to retrieve the result to get any server-side errors.
      writer.getResult();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * A FlightProducer that always produces a fixed data stream with metadata on the side.
   */
  private static class MetadataFlightProducer extends NoOpFlightProducer {

    private final BufferAllocator allocator;

    public MetadataFlightProducer(BufferAllocator allocator) {
      this.allocator = allocator;
    }

    @Override
    public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
      final Schema schema = new Schema(Collections.singletonList(Field.nullable("a", new ArrowType.Int(32, true))));
      try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
        root.allocateNew();
        listener.start(root);
        for (byte i = 0; i < 10; i++) {
          final IntVector vector = (IntVector) root.getVector("a");
          vector.set(0, 10);
          vector.setValueCount(1);
          root.setRowCount(1);
          listener.putNext(new byte[]{i});
        }
        listener.completed();
      }
    }

    @Override
    public Runnable acceptPut(CallContext context, FlightStream stream, StreamListener<PutResult> ackStream) {
      return () -> {
        try (FlightStream flightStream = stream) {
          byte current = 0;
          while (flightStream.next()) {
            byte[] metadata = flightStream.getLatestMetadata();
            if (!Arrays.equals(new byte[]{current}, metadata)) {
              throw new IllegalArgumentException("Metadata does not match expected value.");
            }
            ackStream.onNext(PutResult.metadata(new byte[]{current}));
            current++;
          }
          if (current != 10) {
            throw new IllegalArgumentException("Wrong number of messages sent.");
          }
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      };
    }
  }
}
