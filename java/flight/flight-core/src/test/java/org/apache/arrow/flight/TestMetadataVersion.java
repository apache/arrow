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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.types.MetadataVersion;
import org.apache.arrow.vector.types.UnionMode;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Test clients/servers with different metadata versions.
 */
public class TestMetadataVersion {
  private static BufferAllocator allocator;
  private static Schema schema;
  private static IpcOption optionV4;
  private static IpcOption optionV5;
  private static Schema unionSchema;

  @BeforeAll
  public static void setUpClass() {
    allocator = new RootAllocator(Integer.MAX_VALUE);
    schema = new Schema(Collections.singletonList(Field.nullable("foo", new ArrowType.Int(32, true))));
    unionSchema = new Schema(
        Collections.singletonList(Field.nullable("union", new ArrowType.Union(UnionMode.Dense, new int[]{0}))));

    // avoid writing legacy ipc format by default
    optionV4 = new IpcOption(false, MetadataVersion.V4);
    optionV5 = IpcOption.DEFAULT;
  }

  @AfterAll
  public static void tearDownClass() {
    allocator.close();
  }

  @Test
  public void testGetFlightInfoV4() throws Exception {
    try (final FlightServer server = startServer(optionV4);
         final FlightClient client = connect(server)) {
      final FlightInfo result = client.getInfo(FlightDescriptor.command(new byte[0]));
      assertEquals(Optional.of(schema), result.getSchemaOptional());
    }
  }

  @Test
  public void testGetSchemaV4() throws Exception {
    try (final FlightServer server = startServer(optionV4);
         final FlightClient client = connect(server)) {
      final SchemaResult result = client.getSchema(FlightDescriptor.command(new byte[0]));
      assertEquals(schema, result.getSchema());
    }
  }

  @Test
  public void testUnionCheck() throws Exception {
    assertThrows(IllegalArgumentException.class, () -> new SchemaResult(unionSchema, optionV4));
    assertThrows(IllegalArgumentException.class, () ->
        new FlightInfo(unionSchema, FlightDescriptor.command(new byte[0]), Collections.emptyList(), -1, -1, optionV4));
    try (final FlightServer server = startServer(optionV4);
         final FlightClient client = connect(server);
         final FlightStream stream = client.getStream(new Ticket("union".getBytes(StandardCharsets.UTF_8)))) {
      final FlightRuntimeException err = assertThrows(FlightRuntimeException.class, stream::next);
      assertTrue(err.getMessage().contains("Cannot write union with V4 metadata"), err.getMessage());
    }

    try (final FlightServer server = startServer(optionV4);
         final FlightClient client = connect(server);
         final VectorSchemaRoot root = VectorSchemaRoot.create(unionSchema, allocator)) {
      final FlightDescriptor descriptor = FlightDescriptor.command(new byte[0]);
      final SyncPutListener reader = new SyncPutListener();
      final FlightClient.ClientStreamListener listener = client.startPut(descriptor, reader);
      final IllegalArgumentException err = assertThrows(IllegalArgumentException.class,
          () -> listener.start(root, null, optionV4));
      assertTrue(err.getMessage().contains("Cannot write union with V4 metadata"), err.getMessage());
    }
  }

  @Test
  public void testPutV4() throws Exception {
    try (final FlightServer server = startServer(optionV4);
         final FlightClient client = connect(server);
         final VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      generateData(root);
      final FlightDescriptor descriptor = FlightDescriptor.command(new byte[0]);
      final SyncPutListener reader = new SyncPutListener();
      final FlightClient.ClientStreamListener listener = client.startPut(descriptor, reader);
      listener.start(root, null, optionV4);
      listener.putNext();
      listener.completed();
      listener.getResult();
    }
  }

  @Test
  public void testGetV4() throws Exception {
    try (final FlightServer server = startServer(optionV4);
         final FlightClient client = connect(server);
         final FlightStream stream = client.getStream(new Ticket(new byte[0]))) {
      assertTrue(stream.next());
      assertEquals(optionV4.metadataVersion, stream.metadataVersion);
      validateRoot(stream.getRoot());
      assertFalse(stream.next());
    }
  }

  @Test
  public void testExchangeV4ToV5() throws Exception {
    try (final FlightServer server = startServer(optionV5);
         final FlightClient client = connect(server);
         final VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
         final FlightClient.ExchangeReaderWriter stream = client.doExchange(FlightDescriptor.command(new byte[0]))) {
      stream.getWriter().start(root, null, optionV4);
      generateData(root);
      stream.getWriter().putNext();
      stream.getWriter().completed();
      assertTrue(stream.getReader().next());
      assertEquals(optionV5.metadataVersion, stream.getReader().metadataVersion);
      validateRoot(stream.getReader().getRoot());
      assertFalse(stream.getReader().next());
    }
  }

  @Test
  public void testExchangeV5ToV4() throws Exception {
    try (final FlightServer server = startServer(optionV4);
         final FlightClient client = connect(server);
         final VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
         final FlightClient.ExchangeReaderWriter stream = client.doExchange(FlightDescriptor.command(new byte[0]))) {
      stream.getWriter().start(root, null, optionV5);
      generateData(root);
      stream.getWriter().putNext();
      stream.getWriter().completed();
      assertTrue(stream.getReader().next());
      assertEquals(optionV4.metadataVersion, stream.getReader().metadataVersion);
      validateRoot(stream.getReader().getRoot());
      assertFalse(stream.getReader().next());
    }
  }

  @Test
  public void testExchangeV4ToV4() throws Exception {
    try (final FlightServer server = startServer(optionV4);
         final FlightClient client = connect(server);
         final VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
         final FlightClient.ExchangeReaderWriter stream = client.doExchange(FlightDescriptor.command(new byte[0]))) {
      stream.getWriter().start(root, null, optionV4);
      generateData(root);
      stream.getWriter().putNext();
      stream.getWriter().completed();
      assertTrue(stream.getReader().next());
      assertEquals(optionV4.metadataVersion, stream.getReader().metadataVersion);
      validateRoot(stream.getReader().getRoot());
      assertFalse(stream.getReader().next());
    }
  }

  private static void generateData(VectorSchemaRoot root) {
    assertEquals(schema, root.getSchema());
    final IntVector vector = (IntVector) root.getVector("foo");
    vector.setSafe(0, 0);
    vector.setSafe(1, 1);
    vector.setSafe(2, 4);
    root.setRowCount(3);
  }

  private static void validateRoot(VectorSchemaRoot root) {
    assertEquals(schema, root.getSchema());
    assertEquals(3, root.getRowCount());
    final IntVector vector = (IntVector) root.getVector("foo");
    assertEquals(0, vector.get(0));
    assertEquals(1, vector.get(1));
    assertEquals(4, vector.get(2));
  }

  FlightServer startServer(IpcOption option) throws Exception {
    Location location = Location.forGrpcInsecure(FlightTestUtil.LOCALHOST, 0);
    VersionFlightProducer producer = new VersionFlightProducer(allocator, option);
    final FlightServer server = FlightServer.builder(allocator, location, producer).build();
    server.start();
    return server;
  }

  FlightClient connect(FlightServer server) {
    Location location = Location.forGrpcInsecure(FlightTestUtil.LOCALHOST, server.getPort());
    return FlightClient.builder(allocator, location).build();
  }

  static final class VersionFlightProducer extends NoOpFlightProducer {
    private final BufferAllocator allocator;
    private final IpcOption option;

    VersionFlightProducer(BufferAllocator allocator, IpcOption option) {
      this.allocator = allocator;
      this.option = option;
    }

    @Override
    public FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor) {
      return new FlightInfo(schema, descriptor, Collections.emptyList(), -1, -1, option);
    }

    @Override
    public SchemaResult getSchema(CallContext context, FlightDescriptor descriptor) {
      return new SchemaResult(schema, option);
    }

    @Override
    public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
      if (Arrays.equals("union".getBytes(StandardCharsets.UTF_8), ticket.getBytes())) {
        try (final VectorSchemaRoot root = VectorSchemaRoot.create(unionSchema, allocator)) {
          listener.start(root, null, option);
        } catch (IllegalArgumentException e) {
          listener.error(CallStatus.INTERNAL.withCause(e).withDescription(e.getMessage()).toRuntimeException());
          return;
        }
        listener.error(CallStatus.INTERNAL.withDescription("Expected exception not raised").toRuntimeException());
        return;
      }
      try (final VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
        listener.start(root, null, option);
        generateData(root);
        listener.putNext();
        listener.completed();
      }
    }

    @Override
    public Runnable acceptPut(CallContext context, FlightStream flightStream, StreamListener<PutResult> ackStream) {
      return () -> {
        try {
          assertTrue(flightStream.next());
          assertEquals(option.metadataVersion, flightStream.metadataVersion);
          validateRoot(flightStream.getRoot());
        } catch (AssertionError err) {
          // gRPC doesn't propagate stack traces across the wire.
          err.printStackTrace();
          ackStream.onError(CallStatus.INVALID_ARGUMENT
              .withCause(err)
              .withDescription("Server assertion failed: " + err)
              .toRuntimeException());
          return;
        } catch (RuntimeException err) {
          err.printStackTrace();
          ackStream.onError(CallStatus.INTERNAL
              .withCause(err)
              .withDescription("Server assertion failed: " + err)
              .toRuntimeException());
          return;
        }
        ackStream.onCompleted();
      };
    }

    @Override
    public void doExchange(CallContext context, FlightStream reader, ServerStreamListener writer) {
      try (final VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
        try {
          assertTrue(reader.next());
          validateRoot(reader.getRoot());
          assertFalse(reader.next());
        } catch (AssertionError err) {
          // gRPC doesn't propagate stack traces across the wire.
          err.printStackTrace();
          writer.error(CallStatus.INVALID_ARGUMENT
              .withCause(err)
              .withDescription("Server assertion failed: " + err)
              .toRuntimeException());
          return;
        } catch (RuntimeException err) {
          err.printStackTrace();
          writer.error(CallStatus.INTERNAL
              .withCause(err)
              .withDescription("Server assertion failed: " + err)
              .toRuntimeException());
          return;
        }

        writer.start(root, null, option);
        generateData(root);
        writer.putNext();
        writer.completed();
      }
    }
  }
}
