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
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import org.apache.arrow.flight.FlightClient.PutListener;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Tests for application-specific metadata support in Flight.
 */
public class TestApplicationMetadata {

  // The command used to trigger the test for ARROW-6136.
  private static final byte[] COMMAND_ARROW_6136 = "ARROW-6136".getBytes();
  // The expected error message.
  private static final String MESSAGE_ARROW_6136 = "The stream should not be double-closed.";

  /**
   * Ensure that a client can read the metadata sent from the server.
   */
  @Test
  // This test is consistently flaky on CI, unfortunately.
  @Disabled
  public void retrieveMetadata() {
    test((allocator, client) -> {
      try (final FlightStream stream = client.getStream(new Ticket(new byte[0]))) {
        byte i = 0;
        while (stream.next()) {
          final IntVector vector = (IntVector) stream.getRoot().getVector("a");
          Assertions.assertEquals(1, vector.getValueCount());
          Assertions.assertEquals(10, vector.get(0));
          Assertions.assertEquals(i, stream.getLatestMetadata().getByte(0));
          i++;
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
  }

  /** ARROW-6136: make sure that the Flight implementation doesn't double-close the server-to-client stream. */
  @Test
  public void arrow6136() {
    final Schema schema = new Schema(Collections.emptyList());
    test((allocator, client) -> {
      try (final VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
        final FlightDescriptor descriptor = FlightDescriptor.command(COMMAND_ARROW_6136);

        final PutListener listener = new SyncPutListener();
        final FlightClient.ClientStreamListener writer = client.startPut(descriptor, root, listener);
        // Must attempt to retrieve the result to get any server-side errors.
        final CallStatus status = FlightTestUtil.assertCode(FlightStatusCode.INTERNAL, writer::getResult);
        Assertions.assertEquals(MESSAGE_ARROW_6136, status.description());
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
  }

  /**
   * Ensure that a client can send metadata to the server.
   */
  @Test
  @Disabled
  public void uploadMetadataAsync() {
    final Schema schema = new Schema(Collections.singletonList(Field.nullable("a", new ArrowType.Int(32, true))));
    test((allocator, client) -> {
      try (final VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
        final FlightDescriptor descriptor = FlightDescriptor.path("test");

        final PutListener listener = new AsyncPutListener() {
          int counter = 0;

          @Override
          public void onNext(PutResult val) {
            Assertions.assertNotNull(val);
            Assertions.assertEquals(counter, val.getApplicationMetadata().getByte(0));
            counter++;
          }
        };
        final FlightClient.ClientStreamListener writer = client.startPut(descriptor, root, listener);

        root.allocateNew();
        for (byte i = 0; i < 10; i++) {
          final IntVector vector = (IntVector) root.getVector("a");
          final ArrowBuf metadata = allocator.buffer(1);
          metadata.writeByte(i);
          vector.set(0, 10);
          vector.setValueCount(1);
          root.setRowCount(1);
          writer.putNext(metadata);
        }
        writer.completed();
        // Must attempt to retrieve the result to get any server-side errors.
        writer.getResult();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
  }

  /**
   * Ensure that a client can send metadata to the server. Uses the synchronous API.
   */
  @Test
  @Disabled
  public void uploadMetadataSync() {
    final Schema schema = new Schema(Collections.singletonList(Field.nullable("a", new ArrowType.Int(32, true))));
    test((allocator, client) -> {
      try (final VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
          final SyncPutListener listener = new SyncPutListener()) {
        final FlightDescriptor descriptor = FlightDescriptor.path("test");
        final FlightClient.ClientStreamListener writer = client.startPut(descriptor, root, listener);

        root.allocateNew();
        for (byte i = 0; i < 10; i++) {
          final IntVector vector = (IntVector) root.getVector("a");
          final ArrowBuf metadata = allocator.buffer(1);
          metadata.writeByte(i);
          vector.set(0, 10);
          vector.setValueCount(1);
          root.setRowCount(1);
          writer.putNext(metadata);
          try (final PutResult message = listener.poll(5000, TimeUnit.SECONDS)) {
            Assertions.assertNotNull(message);
            Assertions.assertEquals(i, message.getApplicationMetadata().getByte(0));
          } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
          }
        }
        writer.completed();
        // Must attempt to retrieve the result to get any server-side errors.
        writer.getResult();
      }
    });
  }

  /**
   * Make sure that a {@link SyncPutListener} properly reclaims memory if ignored.
   */
  @Test
  @Disabled
  public void syncMemoryReclaimed() {
    final Schema schema = new Schema(Collections.singletonList(Field.nullable("a", new ArrowType.Int(32, true))));
    test((allocator, client) -> {
      try (final VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
          final SyncPutListener listener = new SyncPutListener()) {
        final FlightDescriptor descriptor = FlightDescriptor.path("test");
        final FlightClient.ClientStreamListener writer = client.startPut(descriptor, root, listener);

        root.allocateNew();
        for (byte i = 0; i < 10; i++) {
          final IntVector vector = (IntVector) root.getVector("a");
          final ArrowBuf metadata = allocator.buffer(1);
          metadata.writeByte(i);
          vector.set(0, 10);
          vector.setValueCount(1);
          root.setRowCount(1);
          writer.putNext(metadata);
        }
        writer.completed();
        // Must attempt to retrieve the result to get any server-side errors.
        writer.getResult();
      }
    });
  }

  /**
   * ARROW-9221: Flight copies metadata from the byte buffer of a Protobuf ByteString,
   * which is in big-endian by default, thus mangling metadata.
   */
  @Test
  public void testMetadataEndianness() throws Exception {
    try (final BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
         final BufferAllocator serverAllocator = allocator.newChildAllocator("flight-server", 0, Long.MAX_VALUE);
         final FlightServer server = FlightServer.builder(serverAllocator, forGrpcInsecure(LOCALHOST, 0),
             new EndianFlightProducer(serverAllocator)).build().start();
         final FlightClient client = FlightClient.builder(allocator, server.getLocation()).build()) {
      final Schema schema = new Schema(Collections.emptyList());
      final FlightDescriptor descriptor = FlightDescriptor.command(new byte[0]);
      try (final SyncPutListener reader = new SyncPutListener();
           final VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
        final FlightClient.ClientStreamListener writer = client.startPut(descriptor, root, reader);
        writer.completed();
        try (final PutResult metadata = reader.read()) {
          Assertions.assertEquals(16, metadata.getApplicationMetadata().readableBytes());
          byte[] bytes = new byte[16];
          metadata.getApplicationMetadata().readBytes(bytes);
          Assertions.assertArrayEquals(EndianFlightProducer.EXPECTED_BYTES, bytes);
        }
        writer.getResult();
      }
    }
  }

  private void test(BiConsumer<BufferAllocator, FlightClient> fun) {
    try (final BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
         final FlightServer s = FlightServer.builder(allocator, forGrpcInsecure(LOCALHOST, 0),
             new MetadataFlightProducer(allocator)).build().start();
         final FlightClient client = FlightClient.builder(allocator, s.getLocation()).build()) {
      fun.accept(allocator, client);
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
          final ArrowBuf metadata = allocator.buffer(1);
          metadata.writeByte(i);
          listener.putNext(metadata);
        }
        listener.completed();
      }
    }

    @Override
    public Runnable acceptPut(CallContext context, FlightStream stream, StreamListener<PutResult> ackStream) {
      return () -> {
        // Wait for the descriptor to be sent
        stream.getRoot();
        if (stream.getDescriptor().isCommand() &&
            Arrays.equals(stream.getDescriptor().getCommand(), COMMAND_ARROW_6136)) {
          // ARROW-6136: Try closing the stream
          ackStream.onError(
              CallStatus.INTERNAL.withDescription(MESSAGE_ARROW_6136).toRuntimeException());
          return;
        }
        try {
          byte current = 0;
          while (stream.next()) {
            final ArrowBuf metadata = stream.getLatestMetadata();
            if (current != metadata.getByte(0)) {
              ackStream.onError(CallStatus.INVALID_ARGUMENT.withDescription(String
                  .format("Metadata does not match expected value; got %d but expected %d.", metadata.getByte(0),
                      current)).toRuntimeException());
              return;
            }
            ackStream.onNext(PutResult.metadata(metadata));
            current++;
          }
          if (current != 10) {
            throw CallStatus.INVALID_ARGUMENT.withDescription("Wrong number of messages sent.").toRuntimeException();
          }
        } catch (Exception e) {
          throw CallStatus.INTERNAL.withCause(e).withDescription(e.toString()).toRuntimeException();
        }
      };
    }
  }

  private static class EndianFlightProducer extends NoOpFlightProducer {
    static final byte[] EXPECTED_BYTES = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
    private final BufferAllocator allocator;

    private EndianFlightProducer(BufferAllocator allocator) {
      this.allocator = allocator;
    }

    @Override
    public Runnable acceptPut(CallContext context, FlightStream flightStream, StreamListener<PutResult> ackStream) {
      return () -> {
        while (flightStream.next()) {
          // Ignore any data
        }

        try (final ArrowBuf buf = allocator.buffer(16)) {
          buf.writeBytes(EXPECTED_BYTES);
          ackStream.onNext(PutResult.metadata(buf));
        }
        ackStream.onCompleted();
      };
    }
  }
}
