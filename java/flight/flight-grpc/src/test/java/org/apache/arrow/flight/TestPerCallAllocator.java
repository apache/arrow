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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.apache.arrow.flight.auth.ServerAuthHandler;
import org.apache.arrow.flight.impl.FlightServiceGrpc;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Test;

import com.google.common.collect.Iterables;

import io.grpc.BindableService;
import io.grpc.HandlerRegistry;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerCall;
import io.grpc.ServerMethodDefinition;

public class TestPerCallAllocator {
  @Test
  public void testDoGet() throws Exception {
    try (final BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE)) {
      test(allocator, (client) -> {
        try (final FlightStream stream = client.getStream(new Ticket(Producer.TICKET_EMPTY))) {
          assertEquals(Producer.SCHEMA_DOUBLES, stream.getRoot().getSchema());
          assertTrue(stream.next());
          assertFalse(stream.next());
        }
      }, (call, serverAllocator) -> {
          // gRPC server shutdown does not actually wait for all onCompleted/onCancel callbacks.
          // Test that we handle this properly by forcing those callbacks to take a while.
          // This does increase test runtime by the given duration here.
          try {
            Thread.sleep(1500);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        });
    }
  }

  @Test
  public void testDoGetError() throws Exception {
    try (final BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE)) {
      test(allocator, (client) -> {
        try (final FlightStream stream = client.getStream(new Ticket(Producer.TICKET_ERROR))) {
          final FlightRuntimeException err = assertThrows(FlightRuntimeException.class, stream::getRoot);
          assertEquals("expected", err.status().description());
        }
      }, (call, serverAllocator) -> { });
    }
  }

  @Test
  public void testDoGetInfinite() throws Exception {
    try (final BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE)) {
      test(allocator, (client) -> {
        try (final FlightStream stream = client.getStream(new Ticket(Producer.TICKET_INFINITE))) {
          assertEquals(Producer.SCHEMA_DOUBLES, stream.getRoot().getSchema());
          assertTrue(stream.next());
          assertTrue(stream.next());
          assertTrue(stream.next());
          stream.cancel("", null);
        }
      }, (call, serverAllocator) -> {
          // gRPC server shutdown does not actually wait for all onCompleted/onCancel callbacks.
          // Test that we handle this properly by forcing those callbacks to take a while.
          try {
            Thread.sleep(2500);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        });
    }
  }

  @Test
  public void testDoPut() throws Exception {
    int batches = 3;
    int rowsPerBatch = 512;
    byte[] command = "dataset-doubles".getBytes(StandardCharsets.US_ASCII);
    try (final BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE)) {
      test(allocator, (client) -> {
        FlightDescriptor descriptor = FlightDescriptor.command(command);
        SyncPutListener reader = new SyncPutListener();
        try (final VectorSchemaRoot root = VectorSchemaRoot.create(Producer.SCHEMA_DOUBLES, allocator)) {
          FlightClient.ClientStreamListener writer = client.startPut(descriptor, root, reader);
          double counter = 0.0;
          Float8Vector vector = (Float8Vector) root.getVector(0);
          for (int batch = 0; batch < batches; batch++) {
            for (int row = 0; row < rowsPerBatch; row++) {
              vector.setSafe(row, counter);
              counter++;
            }
            root.setRowCount(rowsPerBatch);
            writer.putNext();
          }
          writer.completed();
          reader.getResult();
        }

        try (final FlightStream fs = client.getStream(new Ticket(command))) {
          double counter = 0.0;
          assertEquals(Producer.SCHEMA_DOUBLES, fs.getSchema());
          Float8Vector vector = (Float8Vector) fs.getRoot().getVector(0);
          for (int batch = 0; batch < batches; batch++) {
            assertTrue(fs.next());
            assertEquals(rowsPerBatch, fs.getRoot().getRowCount());
            for (int row = 0; row < rowsPerBatch; row++) {
              assertFalse(vector.isNull(row));
              assertEquals(counter, vector.get(row), /* delta */0.1);
              counter++;
            }
          }
          assertFalse(fs.next());
        }
      }, (call, serverAllocator) -> {
          // gRPC server shutdown does not actually wait for all onCompleted/onCancel callbacks.
          // Test that we handle this properly by forcing those callbacks to take a while.
          try {
            Thread.sleep(2500);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        });
    }
  }

  /** Test that the server cleans up Arrow data even if the producer doesn't drain the stream. */
  @Test
  public void testDoPutIgnore() throws Exception {
    try (final BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE)) {
      test(allocator, (client) -> {
        SyncPutListener reader = new SyncPutListener();
        try (final VectorSchemaRoot root = VectorSchemaRoot.create(Producer.SCHEMA_DOUBLES, allocator)) {
          FlightClient.ClientStreamListener writer =
              client.startPut(FlightDescriptor.command(Producer.TICKET_IGNORE), root, reader);
          root.setRowCount(1024);
          writer.putNext();
          writer.putNext();
          writer.putNext();
          writer.completed();
          reader.getResult();
        }

        reader = new SyncPutListener();
        try (final VectorSchemaRoot root = VectorSchemaRoot.create(Producer.SCHEMA_DOUBLES, allocator)) {
          FlightClient.ClientStreamListener writer =
              client.startPut(FlightDescriptor.command(Producer.TICKET_CANCEL), root, reader);
          root.setRowCount(1024);
          writer.putNext();
          writer.putNext();
          writer.putNext();
          writer.completed();
          FlightRuntimeException err = assertThrows(FlightRuntimeException.class, reader::getResult);
          assertEquals(err.toString(), FlightStatusCode.CANCELLED, err.status().code());
        }
      }, (call, serverAllocator) -> { });
    }
  }


  /** Test that the server cleans up Arrow data even if the producer doesn't drain the stream. */
  @Test
  public void testDoExchangeIgnore() throws Exception {
    try (final BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE)) {
      test(allocator, (client) -> {
        FlightDescriptor descriptor = FlightDescriptor.command(Producer.TICKET_IGNORE);
        try (final VectorSchemaRoot root = VectorSchemaRoot.create(Producer.SCHEMA_DOUBLES, allocator);
             FlightClient.ExchangeReaderWriter stream = client.doExchange(descriptor)) {
          stream.getWriter().start(root);
          root.setRowCount(1024);
          stream.getWriter().putNext();
          stream.getWriter().putNext();
          stream.getWriter().putNext();
          stream.getWriter().completed();
          while (stream.getReader().next()) {
            // Drain the stream
          }
        }
      }, (call, serverAllocator) -> { });
    }
  }

  @Test
  public void testDoExchangeInfinite() throws Exception {
    try (final BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE)) {
      test(allocator, (client) -> {
        try (final FlightClient.ExchangeReaderWriter stream =
                 client.doExchange(FlightDescriptor.command(Producer.TICKET_INFINITE))) {
          assertEquals(Producer.SCHEMA_DOUBLES, stream.getReader().getSchema());
          assertTrue(stream.getReader().next());
          assertTrue(stream.getReader().next());
          assertTrue(stream.getReader().next());
          stream.getReader().cancel("", null);
        }
      }, (call, serverAllocator) -> {
          // gRPC server shutdown does not actually wait for all onCompleted/onCancel callbacks.
          // Test that we handle this properly by forcing those callbacks to take a while.
          try {
            Thread.sleep(2500);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        });
    }
  }

  @Test
  public void testDoExchangeError() throws Exception {
    try (final BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE)) {
      test(allocator, (client) -> {
        try (final FlightClient.ExchangeReaderWriter stream =
                 client.doExchange(FlightDescriptor.command(Producer.TICKET_ERROR))) {
          final FlightRuntimeException err = assertThrows(FlightRuntimeException.class, stream.getReader()::next);
          assertEquals("expected", err.status().description());
        }

        try (final FlightClient.ExchangeReaderWriter stream =
                 client.doExchange(FlightDescriptor.command(Producer.TICKET_CANCEL));
             VectorSchemaRoot root = VectorSchemaRoot.create(Producer.SCHEMA_DOUBLES, allocator)) {
          stream.getWriter().start(root);
          stream.getWriter().putNext();
          final FlightRuntimeException err = assertThrows(FlightRuntimeException.class, stream.getReader()::next);
          assertEquals(err.toString(), "expected", err.status().description());
        }
      }, (call, serverAllocator) -> { });
    }
  }

  /** Ensure the custom registry properly hooks up request/response marshallers. */
  @Test
  public void testRegistry() {
    try (final BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE)) {
      final Producer producer = new Producer(allocator);
      final ExecutorService executor = Executors.newCachedThreadPool();
      final BindableService service =
          FlightGrpcUtils.createFlightService(allocator, producer, ServerAuthHandler.NO_OP, executor);
      final HandlerRegistry registry = FlightGrpcUtils.createHandlerRegistry(allocator, service);

      checkMethodDefinition(
           registry.lookupMethod(FlightServiceGrpc.getDoGetMethod().getFullMethodName()),
           /* requestMarshallerOwnsAllocator */ false,
           /* responseMarshallerOwnsAllocator */ true);
      checkMethodDefinition(
          registry.lookupMethod(FlightServiceGrpc.getDoPutMethod().getFullMethodName()),
          /* requestMarshallerOwnsAllocator */ true,
          /* responseMarshallerOwnsAllocator */ false);
      checkMethodDefinition(
          registry.lookupMethod(FlightServiceGrpc.getDoExchangeMethod().getFullMethodName()),
          /* requestMarshallerOwnsAllocator */ true,
          /* responseMarshallerOwnsAllocator */ true);
      assertNotNull(registry.lookupMethod(FlightServiceGrpc.getDoActionMethod().getFullMethodName()));
      assertNull(registry.lookupMethod("/unknown.Service/Unknown"));
    }
  }

  /**
   * Check that the given method definition has the right server call handler/marshallers defined.
   * @param methodDefinition The method definition to check.
   * @param requestMarshallerOwnsAllocator If true, ensure the request marshaller is an ArrowMessageMarshaller.
   * @param responseMarshallerOwnsAllocator If true, ensure the response marshaller is an ArrowMessageMarshaller.
   */
  private void checkMethodDefinition(
      ServerMethodDefinition<?, ?> methodDefinition,
      boolean requestMarshallerOwnsAllocator,
      boolean responseMarshallerOwnsAllocator) {
    assertNotNull(methodDefinition);
    assertTrue(
        methodDefinition.getServerCallHandler() instanceof FlightHandlerRegistry.AllocatorInjectingServerCallHandler);
    MethodDescriptor.Marshaller<?> requestMarshaller = methodDefinition.getMethodDescriptor().getRequestMarshaller();
    MethodDescriptor.Marshaller<?> responseMarshaller = methodDefinition.getMethodDescriptor().getResponseMarshaller();
    if (requestMarshallerOwnsAllocator) {
      assertTrue(requestMarshaller instanceof ArrowMessageMarshaller);
      ((ArrowMessageMarshaller) requestMarshaller).close();
    } else {
      assertFalse(requestMarshaller instanceof ArrowMessageMarshaller);
    }
    if (responseMarshallerOwnsAllocator) {
      assertTrue(responseMarshaller instanceof ArrowMessageMarshaller);
      ((ArrowMessageMarshaller) responseMarshaller).close();
    } else {
      assertFalse(responseMarshaller instanceof ArrowMessageMarshaller);
    }
  }

  private void test(
      BufferAllocator allocator,
      CheckedConsumer<FlightClient> test,
      BiConsumer<ServerCall<?, ?>, BufferAllocator> callback)
      throws Exception {
    final Producer producer = new Producer(allocator.newChildAllocator("Producer", 0, allocator.getLimit()));
    final ExecutorService executor = Executors.newCachedThreadPool();
    final BindableService service =
        FlightGrpcUtils.createFlightService(allocator, producer, ServerAuthHandler.NO_OP, executor);
    AllocatorClosingServerInterceptor interceptor = new AllocatorClosingServerInterceptor(callback);
    Server server = ServerBuilder.forPort(0)
        .fallbackHandlerRegistry(FlightGrpcUtils.createHandlerRegistry(allocator, service))
        .intercept(interceptor)
        .build();
    server.start();
    Location location = Location.forGrpcInsecure("localhost", server.getPort());
    try (FlightClient client = FlightClient.builder(allocator, location).build()) {
      test.accept(client);
    } finally {
      server.shutdown();
      server.awaitTermination();
      producer.close();
      executor.shutdown();
      executor.awaitTermination(30, TimeUnit.SECONDS);
      interceptor.awaitTermination(30, TimeUnit.SECONDS);
    }
  }

  interface CheckedConsumer<T> {
    void accept(T t) throws Exception;
  }

  static final class Producer extends NoOpFlightProducer implements AutoCloseable {
    static final Schema SCHEMA_DOUBLES =
        new Schema(Collections.singletonList(
            Field.nullable("doubles", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE))));
    static final byte[] TICKET_ERROR = "error".getBytes(StandardCharsets.US_ASCII);
    static final byte[] TICKET_CANCEL = "cancel".getBytes(StandardCharsets.US_ASCII);
    static final byte[] TICKET_EMPTY = "empty".getBytes(StandardCharsets.US_ASCII);
    static final byte[] TICKET_INFINITE = "infinite".getBytes(StandardCharsets.US_ASCII);
    static final byte[] TICKET_IGNORE = "ignore".getBytes(StandardCharsets.US_ASCII);

    final BufferAllocator sharedAllocator;
    final Map<String, Dataset> datasets;

    Producer(BufferAllocator allocator) {
      this.sharedAllocator = allocator;
      this.datasets = new HashMap<>();
    }

    @Override
    public void close() throws Exception {
      AutoCloseables.close(Iterables.concat(datasets.values(), Collections.singleton(sharedAllocator)));
    }

    BufferAllocator assertHasAllocator(Consumer<Throwable> onError) {
      final BufferAllocator allocator = FlightGrpcUtils.PER_CALL_ALLOCATOR.get();
      if (allocator == null) {
        onError.accept(CallStatus.INTERNAL.withDescription("Per call allocator does not exist").toRuntimeException());
        return null;
      }
      return allocator;
    }

    @Override
    public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
      BufferAllocator allocator = assertHasAllocator(listener::error);
      if (allocator == null) {
        return;
      }
      if (Arrays.equals(ticket.getBytes(), TICKET_ERROR)) {
        listener.error(CallStatus.INTERNAL.withDescription("expected").toRuntimeException());
      } else if (Arrays.equals(ticket.getBytes(), TICKET_INFINITE)) {
        try (final VectorSchemaRoot root = VectorSchemaRoot.create(SCHEMA_DOUBLES, allocator)) {
          listener.start(root);
          while (!listener.isCancelled()) {
            listener.putNext();
            Thread.sleep(500);
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        } finally {
          listener.completed();
        }
      } else if (Arrays.equals(ticket.getBytes(), TICKET_EMPTY)) {
        try (final VectorSchemaRoot root = VectorSchemaRoot.create(SCHEMA_DOUBLES, allocator)) {
          listener.start(root);
          listener.putNext();
        } finally {
          // Must call completed() after closing the root, or else we'll tell gRPC the call finished
          // (causing the allocator to be closed) with outstanding allocations
          listener.completed();
        }
      } else {
        String key = new String(ticket.getBytes());
        Dataset dataset = datasets.get(key);
        if (dataset == null) {
          listener.error(CallStatus.NOT_FOUND.withDescription(key).toRuntimeException());
          return;
        }
        try (final VectorSchemaRoot root = VectorSchemaRoot.create(dataset.schema, allocator)) {
          VectorLoader loader = new VectorLoader(root);
          listener.start(root);
          for (ArrowRecordBatch batch : dataset.batches) {
            loader.load(batch);
            listener.putNext();
          }
          listener.completed();
        }
      }
    }

    @Override
    public Runnable acceptPut(CallContext context, FlightStream flightStream, StreamListener<PutResult> ackStream) {
      return () -> {
        BufferAllocator allocator = assertHasAllocator(ackStream::onError);
        if (allocator == null) {
          return;
        }
        if (Arrays.equals(flightStream.getDescriptor().getCommand(), TICKET_IGNORE)) {
          // Don't drain the stream, but make sure it actually contains data
          flightStream.getRoot();
          ackStream.onCompleted();
        } else if (Arrays.equals(flightStream.getDescriptor().getCommand(), TICKET_CANCEL)) {
          // Don't drain the stream, but make sure it actually contains data
          flightStream.getRoot();
          ackStream.onError(CallStatus.CANCELLED.withDescription("expected").toRuntimeException());
        } else {
          String key = new String(flightStream.getDescriptor().getCommand());
          final VectorUnloader unloader = new VectorUnloader(flightStream.getRoot());
          final List<ArrowRecordBatch> batches = new ArrayList<>();
          while (flightStream.next()) {
            try (ArrowRecordBatch arb = unloader.getRecordBatch()) {
              batches.add(arb.cloneWithTransfer(sharedAllocator));
            }
          }
          datasets.put(key, new Dataset(flightStream.getRoot().getSchema(), batches));
          ackStream.onCompleted();
        }
      };
    }

    @Override
    public void doExchange(CallContext context, FlightStream reader, ServerStreamListener writer) {
      BufferAllocator allocator = assertHasAllocator(writer::error);
      if (allocator == null) {
        return;
      }

      byte[] command = reader.getDescriptor().getCommand();
      if (Arrays.equals(command, TICKET_IGNORE)) {
        writer.completed();
      } else if (Arrays.equals(command, TICKET_ERROR)) {
        writer.error(CallStatus.INTERNAL.withDescription("expected").toRuntimeException());
      } else if (Arrays.equals(reader.getDescriptor().getCommand(), TICKET_CANCEL)) {
        // Don't drain the stream, but make sure it actually contains data
        reader.getRoot();
        writer.error(CallStatus.CANCELLED.withDescription("expected").toRuntimeException());
      } else if (Arrays.equals(command, TICKET_INFINITE)) {
        try (final VectorSchemaRoot root = VectorSchemaRoot.create(SCHEMA_DOUBLES, allocator)) {
          writer.start(root);
          while (!writer.isCancelled()) {
            writer.putNext();
            Thread.sleep(500);
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        } finally {
          writer.completed();
        }
      }
    }
  }

  static final class Dataset implements AutoCloseable {
    final Schema schema;
    final List<ArrowRecordBatch> batches;

    Dataset(Schema schema, List<ArrowRecordBatch> batches) {
      this.schema = schema;
      this.batches = batches;
    }

    @Override
    public void close() throws Exception {
      AutoCloseables.close(batches);
    }
  }
}
