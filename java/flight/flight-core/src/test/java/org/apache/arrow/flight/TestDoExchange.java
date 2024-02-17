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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.stream.IntStream;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.testing.ValueVectorDataPopulator;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class TestDoExchange {
  static byte[] EXCHANGE_DO_GET = "do-get".getBytes(StandardCharsets.UTF_8);
  static byte[] EXCHANGE_DO_PUT = "do-put".getBytes(StandardCharsets.UTF_8);
  static byte[] EXCHANGE_ECHO = "echo".getBytes(StandardCharsets.UTF_8);
  static byte[] EXCHANGE_METADATA_ONLY = "only-metadata".getBytes(StandardCharsets.UTF_8);
  static byte[] EXCHANGE_TRANSFORM = "transform".getBytes(StandardCharsets.UTF_8);
  static byte[] EXCHANGE_CANCEL = "cancel".getBytes(StandardCharsets.UTF_8);
  static byte[] EXCHANGE_ERROR = "error".getBytes(StandardCharsets.UTF_8);

  private BufferAllocator allocator;
  private FlightServer server;
  private FlightClient client;

  @BeforeEach
  public void setUp() throws Exception {
    allocator = new RootAllocator(Integer.MAX_VALUE);
    final Location serverLocation = Location.forGrpcInsecure(FlightTestUtil.LOCALHOST, 0);
    server = FlightServer.builder(allocator, serverLocation, new Producer(allocator)).build();
    server.start();
    final Location clientLocation = Location.forGrpcInsecure(FlightTestUtil.LOCALHOST, server.getPort());
    client = FlightClient.builder(allocator, clientLocation).build();
  }

  @AfterEach
  public void tearDown() throws Exception {
    AutoCloseables.close(client, server, allocator);
  }

  /** Test a pure-metadata flow. */
  @Test
  public void testDoExchangeOnlyMetadata() throws Exception {
    // Send a particular descriptor to the server and check for a particular response pattern.
    try (final FlightClient.ExchangeReaderWriter stream =
             client.doExchange(FlightDescriptor.command(EXCHANGE_METADATA_ONLY))) {
      final FlightStream reader = stream.getReader();

      // Server starts by sending a message without data (hence no VectorSchemaRoot should be present)
      assertTrue(reader.next());
      assertFalse(reader.hasRoot());
      assertEquals(42, reader.getLatestMetadata().getInt(0));

      // Write a metadata message to the server (without sending any data)
      ArrowBuf buf = allocator.buffer(4);
      buf.writeInt(84);
      stream.getWriter().putMetadata(buf);

      // Check that the server echoed the metadata back to us
      assertTrue(reader.next());
      assertFalse(reader.hasRoot());
      assertEquals(84, reader.getLatestMetadata().getInt(0));

      // Close our write channel and ensure the server also closes theirs
      stream.getWriter().completed();
      assertFalse(reader.next());
    }
  }

  /** Emulate a DoGet with a DoExchange. */
  @Test
  public void testDoExchangeDoGet() throws Exception {
    try (final FlightClient.ExchangeReaderWriter stream =
             client.doExchange(FlightDescriptor.command(EXCHANGE_DO_GET))) {
      final FlightStream reader = stream.getReader();
      VectorSchemaRoot root = reader.getRoot();
      IntVector iv = (IntVector) root.getVector("a");
      int value = 0;
      while (reader.next()) {
        for (int i = 0; i < root.getRowCount(); i++) {
          assertFalse(iv.isNull(i), String.format("Row %d should not be null", value));
          assertEquals(value, iv.get(i));
          value++;
        }
      }
      assertEquals(100, value);
    }
  }

  /** Emulate a DoPut with a DoExchange. */
  @Test
  public void testDoExchangeDoPut() throws Exception {
    final Schema schema = new Schema(Collections.singletonList(Field.nullable("a", new ArrowType.Int(32, true))));
    try (final FlightClient.ExchangeReaderWriter stream =
             client.doExchange(FlightDescriptor.command(EXCHANGE_DO_PUT));
         final VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      IntVector iv = (IntVector) root.getVector("a");
      iv.allocateNew();

      stream.getWriter().start(root);
      int counter = 0;
      for (int i = 0; i < 10; i++) {
        ValueVectorDataPopulator.setVector(iv, IntStream.range(0, i).boxed().toArray(Integer[]::new));
        root.setRowCount(i);
        counter += i;
        stream.getWriter().putNext();

        assertTrue(stream.getReader().next());
        assertFalse(stream.getReader().hasRoot());
        // For each write, the server sends back a metadata message containing the index of the last written batch
        final ArrowBuf metadata = stream.getReader().getLatestMetadata();
        assertEquals(counter, metadata.getInt(0));
      }
      stream.getWriter().completed();

      while (stream.getReader().next()) {
        // Drain the stream. Otherwise closing the stream sends a CANCEL which seriously screws with the server.
        // CANCEL -> runs onCancel handler -> closes the FlightStream early
      }
    }
  }

  /** Test a DoExchange that echoes the client message. */
  @Test
  public void testDoExchangeEcho() throws Exception {
    final Schema schema = new Schema(Collections.singletonList(Field.nullable("a", new ArrowType.Int(32, true))));
    try (final FlightClient.ExchangeReaderWriter stream = client.doExchange(FlightDescriptor.command(EXCHANGE_ECHO));
         final VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      final FlightStream reader = stream.getReader();

      // First try writing metadata without starting the Arrow data stream
      ArrowBuf buf = allocator.buffer(4);
      buf.writeInt(42);
      stream.getWriter().putMetadata(buf);
      buf = allocator.buffer(4);
      buf.writeInt(84);
      stream.getWriter().putMetadata(buf);

      // Ensure that the server echoes the metadata back, also without starting its data stream
      assertTrue(reader.next());
      assertFalse(reader.hasRoot());
      assertEquals(42, reader.getLatestMetadata().getInt(0));
      assertTrue(reader.next());
      assertFalse(reader.hasRoot());
      assertEquals(84, reader.getLatestMetadata().getInt(0));

      // Write data and check that it gets echoed back.
      IntVector iv = (IntVector) root.getVector("a");
      iv.allocateNew();
      stream.getWriter().start(root);
      for (int i = 0; i < 10; i++) {
        iv.setSafe(0, i);
        root.setRowCount(1);
        stream.getWriter().putNext();

        assertTrue(reader.next());
        assertNull(reader.getLatestMetadata());
        assertEquals(root.getSchema(), reader.getSchema());
        assertEquals(i, ((IntVector) reader.getRoot().getVector("a")).get(0));
      }

      // Complete the stream so that the server knows not to expect any more messages from us.
      stream.getWriter().completed();
      // The server will end its side of the call, so this shouldn't block or indicate that
      // there is more data.
      assertFalse(reader.next(), "We should not be waiting for any messages");
    }
  }

  /** Write some data, have it transformed, then read it back. */
  @Test
  public void testTransform() throws Exception {
    final Schema schema = new Schema(Arrays.asList(
        Field.nullable("a", new ArrowType.Int(32, true)),
        Field.nullable("b", new ArrowType.Int(32, true))));
    try (final FlightClient.ExchangeReaderWriter stream =
             client.doExchange(FlightDescriptor.command(EXCHANGE_TRANSFORM))) {
      // Write ten batches of data to the stream, where batch N contains N rows of data (N in [0, 10))
      final FlightStream reader = stream.getReader();
      final FlightClient.ClientStreamListener writer = stream.getWriter();
      try (final VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
        writer.start(root);
        for (int batchIndex = 0; batchIndex < 10; batchIndex++) {
          for (final FieldVector rawVec : root.getFieldVectors()) {
            final IntVector vec = (IntVector) rawVec;
            ValueVectorDataPopulator.setVector(vec, IntStream.range(0, batchIndex).boxed().toArray(Integer[]::new));
          }
          root.setRowCount(batchIndex);
          writer.putNext();
        }
      }
      // Indicate that we're done writing so that the server does not expect more data.
      writer.completed();

      // Read back data. We expect the server to double each value in each row of each batch.
      assertEquals(schema, reader.getSchema());
      final VectorSchemaRoot root = reader.getRoot();
      for (int batchIndex = 0; batchIndex < 10; batchIndex++) {
        assertTrue(reader.next(), "Didn't receive batch #" + batchIndex);
        assertEquals(batchIndex, root.getRowCount());
        for (final FieldVector rawVec : root.getFieldVectors()) {
          final IntVector vec = (IntVector) rawVec;
          for (int row = 0; row < batchIndex; row++) {
            assertEquals(2 * row, vec.get(row));
          }
        }
      }

      // The server also sends back a metadata-only message containing the message count
      assertTrue(reader.next(), "There should be one extra message");
      assertEquals(10, reader.getLatestMetadata().getInt(0));
      assertFalse(reader.next(), "There should be no more data");
    }
  }

  /** Write some data, have it transformed, then read it back. Use the zero-copy optimization. */
  @Test
  public void testTransformZeroCopy() throws Exception {
    final int rowsPerBatch = 4096;
    final Schema schema = new Schema(Arrays.asList(
        Field.nullable("a", new ArrowType.Int(32, true)),
        Field.nullable("b", new ArrowType.Int(32, true))));
    try (final FlightClient.ExchangeReaderWriter stream =
             client.doExchange(FlightDescriptor.command(EXCHANGE_TRANSFORM))) {
      // Write ten batches of data to the stream, where batch N contains 1024 rows of data (N in [0, 10))
      final FlightStream reader = stream.getReader();
      final FlightClient.ClientStreamListener writer = stream.getWriter();
      try (final VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
        writer.start(root);
        // Enable the zero-copy optimization
        writer.setUseZeroCopy(true);
        for (int batchIndex = 0; batchIndex < 100; batchIndex++) {
          for (final FieldVector rawVec : root.getFieldVectors()) {
            final IntVector vec = (IntVector) rawVec;
            for (int row = 0; row < rowsPerBatch; row++) {
              // Use a value that'll be different per batch, so we can detect if we accidentally
              // reuse a buffer (and overwrite a buffer that hasn't yet been sent over the network)
              vec.setSafe(row, batchIndex + row);
            }
          }
          root.setRowCount(rowsPerBatch);
          writer.putNext();
          // Allocate new buffers every time since we don't know if gRPC has written the buffer
          // to the network yet
          root.allocateNew();
        }
      }
      // Indicate that we're done writing so that the server does not expect more data.
      writer.completed();

      // Read back data. We expect the server to double each value in each row of each batch.
      assertEquals(schema, reader.getSchema());
      final VectorSchemaRoot root = reader.getRoot();
      for (int batchIndex = 0; batchIndex < 100; batchIndex++) {
        assertTrue(reader.next(), "Didn't receive batch #" + batchIndex);
        assertEquals(rowsPerBatch, root.getRowCount());
        for (final FieldVector rawVec : root.getFieldVectors()) {
          final IntVector vec = (IntVector) rawVec;
          for (int row = 0; row < rowsPerBatch; row++) {
            assertEquals(2 * (batchIndex + row), vec.get(row));
          }
        }
      }

      // The server also sends back a metadata-only message containing the message count
      assertTrue(reader.next(), "There should be one extra message");
      assertEquals(100, reader.getLatestMetadata().getInt(0));
      assertFalse(reader.next(), "There should be no more data");
    }
  }

  /** Have the server immediately cancel; ensure the client doesn't hang. */
  @Test
  public void testServerCancel() throws Exception {
    try (final FlightClient.ExchangeReaderWriter stream =
             client.doExchange(FlightDescriptor.command(EXCHANGE_CANCEL))) {
      final FlightStream reader = stream.getReader();
      final FlightClient.ClientStreamListener writer = stream.getWriter();

      final FlightRuntimeException fre = assertThrows(FlightRuntimeException.class, reader::next);
      assertEquals(FlightStatusCode.CANCELLED, fre.status().code());
      assertEquals("expected", fre.status().description());

      // Before, this would hang forever, because the writer checks if the stream is ready and not cancelled.
      // However, the cancellation flag (was) only updated by reading, and the stream is never ready once the call ends.
      // The test looks weird since normally, an application shouldn't try to write after the read fails. However,
      // an application that isn't reading data wouldn't notice, and would instead get stuck on the write.
      // Here, we read first to avoid a race condition in the test itself.
      writer.putMetadata(allocator.getEmpty());
    }
  }

  /** Have the server immediately cancel; ensure the server cleans up the FlightStream. */
  @Test
  public void testServerCancelLeak() throws Exception {
    try (final FlightClient.ExchangeReaderWriter stream =
             client.doExchange(FlightDescriptor.command(EXCHANGE_CANCEL))) {
      final FlightStream reader = stream.getReader();
      final FlightClient.ClientStreamListener writer = stream.getWriter();
      try (final VectorSchemaRoot root = VectorSchemaRoot.create(Producer.SCHEMA, allocator)) {
        writer.start(root);
        final IntVector ints = (IntVector) root.getVector("a");
        for (int i = 0; i < 128; i++) {
          for (int row = 0; row < 1024; row++) {
            ints.setSafe(row, row);
          }
          root.setRowCount(1024);
          writer.putNext();
        }
      }

      final FlightRuntimeException fre = assertThrows(FlightRuntimeException.class, reader::next);
      assertEquals(FlightStatusCode.CANCELLED, fre.status().code());
      assertEquals("expected", fre.status().description());
    }
  }

  /** Have the client cancel without reading; ensure memory is not leaked. */
  @Test
  @Disabled
  public void testClientCancel() throws Exception {
    try (final FlightClient.ExchangeReaderWriter stream =
             client.doExchange(FlightDescriptor.command(EXCHANGE_DO_GET))) {
      final FlightStream reader = stream.getReader();
      reader.cancel("", null);
      // Cancel should be idempotent
      reader.cancel("", null);
    }
  }

  /** Test a DoExchange error handling. */
  @Test
  public void testDoExchangeError() throws Exception {
    final Schema schema = new Schema(Collections.singletonList(Field.nullable("a", new ArrowType.Int(32, true))));
    try (final FlightClient.ExchangeReaderWriter stream = client.doExchange(FlightDescriptor.command(EXCHANGE_ERROR));
         final VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      final FlightStream reader = stream.getReader();

      // Write data and check that it gets echoed back.
      IntVector iv = (IntVector) root.getVector("a");
      iv.allocateNew();
      stream.getWriter().start(root);
      for (int i = 0; i < 10; i++) {
        iv.setSafe(0, i);
        root.setRowCount(1);
        stream.getWriter().putNext();

        assertTrue(reader.next());
        assertEquals(root.getSchema(), reader.getSchema());
        assertEquals(i, ((IntVector) reader.getRoot().getVector("a")).get(0));
      }

      // Complete the stream so that the server knows not to expect any more messages from us.
      stream.getWriter().completed();

      // Must call reader.next() to get any errors after exchange, will return false if no error
      final FlightRuntimeException fre = assertThrows(FlightRuntimeException.class, stream::getResult);
      assertEquals("error completing exchange", fre.status().description());
    }
  }

  /** Have the client close the stream without reading; ensure memory is not leaked. */
  @Test
  public void testClientClose() throws Exception {
    try (final FlightClient.ExchangeReaderWriter stream =
             client.doExchange(FlightDescriptor.command(EXCHANGE_DO_GET))) {
      assertEquals(Producer.SCHEMA, stream.getReader().getSchema());
    }
    // Intentionally leak the allocator in this test. gRPC has a bug where it does not wait for all calls to complete
    // when shutting down the server, so this test will fail otherwise because it closes the allocator while the
    // server-side call still has memory allocated.
    // TODO(ARROW-9586): fix this once we track outstanding RPCs outside of gRPC.
    // https://stackoverflow.com/questions/46716024/
    allocator = null;
    client = null;
  }

  /** Test closing with Metadata can't lead to error. */
  @Test
  public void testCloseWithMetadata() throws Exception {
    // Send a particular descriptor to the server and check for a particular response pattern.
    try (final FlightClient.ExchangeReaderWriter stream =
                 client.doExchange(FlightDescriptor.command(EXCHANGE_METADATA_ONLY))) {
      final FlightStream reader = stream.getReader();

      // Server starts by sending a message without data (hence no VectorSchemaRoot should be present)
      assertTrue(reader.next());
      assertFalse(reader.hasRoot());
      assertEquals(42, reader.getLatestMetadata().getInt(0));

      // Write a metadata message to the server (without sending any data)
      ArrowBuf buf = allocator.buffer(4);
      buf.writeInt(84);
      stream.getWriter().putMetadata(buf);

      // Check that the server echoed the metadata back to us
      assertTrue(reader.next());
      assertFalse(reader.hasRoot());
      assertEquals(84, reader.getLatestMetadata().getInt(0));

      // Close our write channel and ensure the server also closes theirs
      stream.getWriter().completed();
      stream.getResult();

      // Not necessary to close reader here, but check closing twice doesn't lead to negative refcnt from metadata
      stream.getReader().close();
    }
  }

  static class Producer extends NoOpFlightProducer {
    static final Schema SCHEMA = new Schema(
        Collections.singletonList(Field.nullable("a", new ArrowType.Int(32, true))));
    private final BufferAllocator allocator;

    Producer(BufferAllocator allocator) {
      this.allocator = allocator;
    }

    @Override
    public void doExchange(CallContext context, FlightStream reader, ServerStreamListener writer) {
      if (Arrays.equals(reader.getDescriptor().getCommand(), EXCHANGE_METADATA_ONLY)) {
        metadataOnly(context, reader, writer);
      } else if (Arrays.equals(reader.getDescriptor().getCommand(), EXCHANGE_DO_GET)) {
        doGet(context, reader, writer);
      } else if (Arrays.equals(reader.getDescriptor().getCommand(), EXCHANGE_DO_PUT)) {
        doPut(context, reader, writer);
      } else if (Arrays.equals(reader.getDescriptor().getCommand(), EXCHANGE_ECHO)) {
        echo(context, reader, writer);
      } else if (Arrays.equals(reader.getDescriptor().getCommand(), EXCHANGE_TRANSFORM)) {
        transform(context, reader, writer);
      } else if (Arrays.equals(reader.getDescriptor().getCommand(), EXCHANGE_CANCEL)) {
        cancel(context, reader, writer);
      } else if (Arrays.equals(reader.getDescriptor().getCommand(), EXCHANGE_ERROR)) {
        error(context, reader, writer);
      } else {
        writer.error(CallStatus.UNIMPLEMENTED.withDescription("Command not implemented").toRuntimeException());
      }
    }

    /** Emulate DoGet. */
    private void doGet(CallContext unusedContext, FlightStream unusedReader, ServerStreamListener writer) {
      try (VectorSchemaRoot root = VectorSchemaRoot.create(SCHEMA, allocator)) {
        writer.start(root);
        root.allocateNew();
        IntVector iv = (IntVector) root.getVector("a");

        for (int i = 0; i < 100; i += 2) {
          iv.set(0, i);
          iv.set(1, i + 1);
          root.setRowCount(2);
          writer.putNext();
        }
      }
      writer.completed();
    }

    /** Emulate DoPut. */
    private void doPut(CallContext unusedContext, FlightStream reader, ServerStreamListener writer) {
      int counter = 0;
      while (reader.next()) {
        if (!reader.hasRoot()) {
          writer.error(CallStatus.INVALID_ARGUMENT.withDescription("Message has no data").toRuntimeException());
          return;
        }
        counter += reader.getRoot().getRowCount();

        final ArrowBuf pong = allocator.buffer(4);
        pong.writeInt(counter);
        writer.putMetadata(pong);
      }
      writer.completed();
    }

    /** Exchange metadata without ever exchanging data. */
    private void metadataOnly(CallContext unusedContext, FlightStream reader, ServerStreamListener writer) {
      final ArrowBuf buf = allocator.buffer(4);
      buf.writeInt(42);
      writer.putMetadata(buf);
      assertTrue(reader.next());
      assertNotNull(reader.getLatestMetadata());
      reader.getLatestMetadata().getReferenceManager().retain();
      writer.putMetadata(reader.getLatestMetadata());
      writer.completed();
    }

    /** Echo the client's response back to it. */
    private void echo(CallContext unusedContext, FlightStream reader, ServerStreamListener writer) {
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

    /** Accept a set of messages, then return some result. */
    private void transform(CallContext unusedContext, FlightStream reader, ServerStreamListener writer) {
      final Schema schema = reader.getSchema();
      for (final Field field : schema.getFields()) {
        if (!(field.getType() instanceof ArrowType.Int)) {
          writer.error(CallStatus.INVALID_ARGUMENT.withDescription("Invalid type: " + field).toRuntimeException());
          return;
        }
        final ArrowType.Int intType = (ArrowType.Int) field.getType();
        if (!intType.getIsSigned() || intType.getBitWidth() != 32) {
          writer.error(CallStatus.INVALID_ARGUMENT.withDescription("Must be i32: " + field).toRuntimeException());
          return;
        }
      }
      int batches = 0;
      try (final VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
        writer.start(root);
        writer.setUseZeroCopy(true);
        final VectorLoader loader = new VectorLoader(root);
        final VectorUnloader unloader = new VectorUnloader(reader.getRoot());
        while (reader.next()) {
          try (final ArrowRecordBatch batch = unloader.getRecordBatch()) {
            loader.load(batch);
          }
          batches++;
          for (final FieldVector rawVec : root.getFieldVectors()) {
            final IntVector vec = (IntVector) rawVec;
            for (int i = 0; i < root.getRowCount(); i++) {
              if (!vec.isNull(i)) {
                vec.set(i, vec.get(i) * 2);
              }
            }
          }
          writer.putNext();
        }
      }
      final ArrowBuf count = allocator.buffer(4);
      count.writeInt(batches);
      writer.putMetadata(count);
      writer.completed();
    }

    /** Immediately cancel the call. */
    private void cancel(CallContext unusedContext, FlightStream unusedReader, ServerStreamListener writer) {
      writer.error(CallStatus.CANCELLED.withDescription("expected").toRuntimeException());
    }

    private void error(CallContext unusedContext, FlightStream reader, ServerStreamListener writer) {
      VectorSchemaRoot root = null;
      VectorLoader loader = null;
      while (reader.next()) {

        if (root == null) {
          root = VectorSchemaRoot.create(reader.getSchema(), allocator);
          loader = new VectorLoader(root);
          writer.start(root);
        }
        VectorUnloader unloader = new VectorUnloader(reader.getRoot());
        try (final ArrowRecordBatch arb = unloader.getRecordBatch()) {
          loader.load(arb);
        }

        writer.putNext();
      }
      if (root != null) {
        root.close();
      }

      // An error occurs before completing the writer
      writer.error(CallStatus.INTERNAL.withDescription("error completing exchange").toRuntimeException());
    }
  }
}
