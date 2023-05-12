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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.apache.arrow.flight.FlightClient.ClientStreamListener;
import org.apache.arrow.flight.impl.Flight;
import org.apache.arrow.flight.impl.Flight.FlightDescriptor.DescriptorType;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.common.base.Charsets;
import com.google.protobuf.ByteString;

import io.grpc.MethodDescriptor;

/**
 * Test the operations of a basic flight service.
 */
public class TestBasicOperation {

  @Test
  public void fastPathDefaults() {
    Assertions.assertTrue(ArrowMessage.ENABLE_ZERO_COPY_READ);
    Assertions.assertFalse(ArrowMessage.ENABLE_ZERO_COPY_WRITE);
  }

  /**
   * ARROW-6017: we should be able to construct locations for unknown schemes.
   */
  @Test
  public void unknownScheme() throws URISyntaxException {
    final Location location = new Location("s3://unknown");
    Assertions.assertEquals("s3", location.getUri().getScheme());
  }

  @Test
  public void unknownSchemeRemote() throws Exception {
    test(c -> {
      try {
        final FlightInfo info = c.getInfo(FlightDescriptor.path("test"));
        Assertions.assertEquals(new URI("https://example.com"), info.getEndpoints().get(0).getLocations().get(0).getUri());
      } catch (URISyntaxException e) {
        throw new RuntimeException(e);
      }
    });
  }

  @Test
  public void roundTripTicket() throws Exception {
    final Ticket ticket = new Ticket(new byte[]{0, 1, 2, 3, 4, 5});
    Assertions.assertEquals(ticket, Ticket.deserialize(ticket.serialize()));
  }

  @Test
  public void roundTripInfo() throws Exception {
    final Map<String, String> metadata = new HashMap<>();
    metadata.put("foo", "bar");
    final Schema schema = new Schema(Arrays.asList(
        Field.nullable("a", new ArrowType.Int(32, true)),
        Field.nullable("b", new ArrowType.FixedSizeBinary(32))
    ), metadata);
    final FlightInfo info1 = new FlightInfo(schema, FlightDescriptor.path(), Collections.emptyList(), -1, -1);
    final FlightInfo info2 = new FlightInfo(schema, FlightDescriptor.command(new byte[2]),
        Collections.singletonList(new FlightEndpoint(
            new Ticket(new byte[10]), Location.forGrpcDomainSocket("/tmp/test.sock"))), 200, 500);
    final FlightInfo info3 = new FlightInfo(schema, FlightDescriptor.path("a", "b"),
        Arrays.asList(new FlightEndpoint(
                new Ticket(new byte[10]), Location.forGrpcDomainSocket("/tmp/test.sock")),
            new FlightEndpoint(
                new Ticket(new byte[10]), Location.forGrpcDomainSocket("/tmp/test.sock"),
                forGrpcInsecure("localhost", 50051))
        ), 200, 500);
    final FlightInfo info4 = new FlightInfo(schema, FlightDescriptor.path("a", "b"),
            Arrays.asList(new FlightEndpoint(
                            new Ticket(new byte[10]), Location.forGrpcDomainSocket("/tmp/test.sock")),
                    new FlightEndpoint(
                            new Ticket(new byte[10]), Location.forGrpcDomainSocket("/tmp/test.sock"),
                            forGrpcInsecure("localhost", 50051))
            ), 200, 500, /*ordered*/ true, IpcOption.DEFAULT);

    Assertions.assertEquals(info1, FlightInfo.deserialize(info1.serialize()));
    Assertions.assertEquals(info2, FlightInfo.deserialize(info2.serialize()));
    Assertions.assertEquals(info3, FlightInfo.deserialize(info3.serialize()));
    Assertions.assertEquals(info4, FlightInfo.deserialize(info4.serialize()));

    Assertions.assertNotEquals(info3, info4);

    Assertions.assertFalse(info1.getOrdered());
    Assertions.assertFalse(info2.getOrdered());
    Assertions.assertFalse(info3.getOrdered());
    Assertions.assertTrue(info4.getOrdered());
  }

  @Test
  public void roundTripDescriptor() throws Exception {
    final FlightDescriptor cmd = FlightDescriptor.command("test command".getBytes(StandardCharsets.UTF_8));
    Assertions.assertEquals(cmd, FlightDescriptor.deserialize(cmd.serialize()));
    final FlightDescriptor path = FlightDescriptor.path("foo", "bar", "test.arrow");
    Assertions.assertEquals(path, FlightDescriptor.deserialize(path.serialize()));
  }

  @Test
  public void getDescriptors() throws Exception {
    test(c -> {
      int count = 0;
      for (FlightInfo i : c.listFlights(Criteria.ALL)) {
        count += 1;
      }
      Assertions.assertEquals(1, count);
    });
  }

  @Test
  public void getDescriptorsWithCriteria() throws Exception {
    test(c -> {
      int count = 0;
      for (FlightInfo i : c.listFlights(new Criteria(new byte[]{1}))) {
        count += 1;
      }
      Assertions.assertEquals(0, count);
    });
  }

  @Test
  public void getDescriptor() throws Exception {
    test(c -> {
      System.out.println(c.getInfo(FlightDescriptor.path("hello")).getDescriptor());
    });
  }

  @Test
  public void getSchema() throws Exception {
    test(c -> {
      System.out.println(c.getSchema(FlightDescriptor.path("hello")).getSchema());
    });
  }


  @Test
  public void listActions() throws Exception {
    test(c -> {
      for (ActionType at : c.listActions()) {
        System.out.println(at.getType());
      }
    });
  }

  @Test
  public void doAction() throws Exception {
    test(c -> {
      Iterator<Result> stream = c.doAction(new Action("hello"));

      Assertions.assertTrue(stream.hasNext());
      Result r = stream.next();
      Assertions.assertArrayEquals("world".getBytes(Charsets.UTF_8), r.getBody());
    });
    test(c -> {
      Iterator<Result> stream = c.doAction(new Action("hellooo"));

      Assertions.assertTrue(stream.hasNext());
      Result r = stream.next();
      Assertions.assertArrayEquals("world".getBytes(Charsets.UTF_8), r.getBody());

      Assertions.assertTrue(stream.hasNext());
      r = stream.next();
      Assertions.assertArrayEquals("!".getBytes(Charsets.UTF_8), r.getBody());
      Assertions.assertFalse(stream.hasNext());
    });
  }

  @Test
  public void putStream() throws Exception {
    test((c, a) -> {
      final int size = 10;

      IntVector iv = new IntVector("c1", a);

      try (VectorSchemaRoot root = VectorSchemaRoot.of(iv)) {
        ClientStreamListener listener = c
            .startPut(FlightDescriptor.path("hello"), root, new AsyncPutListener());

        //batch 1
        root.allocateNew();
        for (int i = 0; i < size; i++) {
          iv.set(i, i);
        }
        iv.setValueCount(size);
        root.setRowCount(size);
        listener.putNext();

        // batch 2

        root.allocateNew();
        for (int i = 0; i < size; i++) {
          iv.set(i, i + size);
        }
        iv.setValueCount(size);
        root.setRowCount(size);
        listener.putNext();
        root.clear();
        listener.completed();

        // wait for ack to avoid memory leaks.
        listener.getResult();
      }
    });
  }

  @Test
  public void propagateErrors() throws Exception {
    test(client -> {
      FlightTestUtil.assertCode(FlightStatusCode.UNIMPLEMENTED, () -> {
        client.doAction(new Action("invalid-action")).forEachRemaining(action -> Assertions.fail());
      });
    });
  }

  @Test
  public void getStream() throws Exception {
    test(c -> {
      try (final FlightStream stream = c.getStream(new Ticket(new byte[0]))) {
        VectorSchemaRoot root = stream.getRoot();
        IntVector iv = (IntVector) root.getVector("c1");
        int value = 0;
        while (stream.next()) {
          for (int i = 0; i < root.getRowCount(); i++) {
            Assertions.assertEquals(value, iv.get(i));
            value++;
          }
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
  }

  /** Ensure the client is configured to accept large messages. */
  @Test
  public void getStreamLargeBatch() throws Exception {
    test(c -> {
      try (final FlightStream stream = c.getStream(new Ticket(Producer.TICKET_LARGE_BATCH))) {
        Assertions.assertEquals(128, stream.getRoot().getFieldVectors().size());
        Assertions.assertTrue(stream.next());
        Assertions.assertEquals(65536, stream.getRoot().getRowCount());
        Assertions.assertTrue(stream.next());
        Assertions.assertEquals(65536, stream.getRoot().getRowCount());
        Assertions.assertFalse(stream.next());
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
  }

  /** Ensure the server is configured to accept large messages. */
  @Test
  public void startPutLargeBatch() throws Exception {
    try (final BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE)) {
      final List<FieldVector> vectors = new ArrayList<>();
      for (int col = 0; col < 128; col++) {
        final BigIntVector vector = new BigIntVector("f" + col, allocator);
        for (int row = 0; row < 65536; row++) {
          vector.setSafe(row, row);
        }
        vectors.add(vector);
      }
      test(c -> {
        try (final VectorSchemaRoot root = new VectorSchemaRoot(vectors)) {
          root.setRowCount(65536);
          final ClientStreamListener stream = c.startPut(FlightDescriptor.path(""), root, new SyncPutListener());
          stream.putNext();
          stream.putNext();
          stream.completed();
          stream.getResult();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      });
    }
  }

  private void test(Consumer<FlightClient> consumer) throws Exception {
    test((c, a) -> {
      consumer.accept(c);
    });
  }

  private void test(BiConsumer<FlightClient, BufferAllocator> consumer) throws Exception {
    try (
        BufferAllocator a = new RootAllocator(Long.MAX_VALUE);
        Producer producer = new Producer(a);
        FlightServer s = FlightServer.builder(a, forGrpcInsecure(LOCALHOST, 0), producer).build().start()) {

      try (
          FlightClient c = FlightClient.builder(a, s.getLocation()).build()
      ) {
        try (BufferAllocator testAllocator = a.newChildAllocator("testcase", 0, Long.MAX_VALUE)) {
          consumer.accept(c, testAllocator);
        }
      }
    }
  }

  /** Helper method to convert an ArrowMessage into a Protobuf message. */
  private Flight.FlightData arrowMessageToProtobuf(
      MethodDescriptor.Marshaller<ArrowMessage> marshaller, ArrowMessage message) throws IOException {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (final InputStream serialized = marshaller.stream(message)) {
      final byte[] buf = new byte[1024];
      while (true) {
        int read = serialized.read(buf);
        if (read < 0) {
          break;
        }
        baos.write(buf, 0, read);
      }
    }
    final byte[] serializedMessage = baos.toByteArray();
    return Flight.FlightData.parseFrom(serializedMessage);
  }

  /** ARROW-10962: accept FlightData messages generated by Protobuf (which can omit empty fields). */
  @Test
  public void testProtobufRecordBatchCompatibility() throws Exception {
    final Schema schema = new Schema(Collections.singletonList(Field.nullable("foo", new ArrowType.Int(32, true))));
    try (final BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
         final VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      final VectorUnloader unloader = new VectorUnloader(root);
      root.setRowCount(0);
      final MethodDescriptor.Marshaller<ArrowMessage> marshaller = ArrowMessage.createMarshaller(allocator);
      try (final ArrowMessage message = new ArrowMessage(
              unloader.getRecordBatch(), /* appMetadata */ null, /* tryZeroCopy */ false, IpcOption.DEFAULT)) {
        Assertions.assertEquals(ArrowMessage.HeaderType.RECORD_BATCH, message.getMessageType());
        // Should have at least one empty body buffer (there may be multiple for e.g. data and validity)
        Iterator<ArrowBuf> iterator = message.getBufs().iterator();
        Assertions.assertTrue(iterator.hasNext());
        while (iterator.hasNext()) {
          Assertions.assertEquals(0, iterator.next().capacity());
        }
        final Flight.FlightData protobufData = arrowMessageToProtobuf(marshaller, message)
            .toBuilder()
            .clearDataBody()
            .build();
        Assertions.assertEquals(0, protobufData.getDataBody().size());
        ArrowMessage parsedMessage = marshaller.parse(new ByteArrayInputStream(protobufData.toByteArray()));
        // Should have an empty body buffer
        Iterator<ArrowBuf> parsedIterator = parsedMessage.getBufs().iterator();
        Assertions.assertTrue(parsedIterator.hasNext());
        Assertions.assertEquals(0, parsedIterator.next().capacity());
        // Should have only one (the parser synthesizes exactly one); in the case of empty buffers, this is equivalent
        Assertions.assertFalse(parsedIterator.hasNext());
        // Should not throw
        final ArrowRecordBatch rb = parsedMessage.asRecordBatch();
        Assertions.assertEquals(rb.computeBodyLength(), 0);
      }
    }
  }

  /** ARROW-10962: accept FlightData messages generated by Protobuf (which can omit empty fields). */
  @Test
  public void testProtobufSchemaCompatibility() throws Exception {
    final Schema schema = new Schema(Collections.singletonList(Field.nullable("foo", new ArrowType.Int(32, true))));
    try (final BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE)) {
      final MethodDescriptor.Marshaller<ArrowMessage> marshaller = ArrowMessage.createMarshaller(allocator);
      Flight.FlightDescriptor descriptor = FlightDescriptor.command(new byte[0]).toProtocol();
      try (final ArrowMessage message = new ArrowMessage(descriptor, schema, IpcOption.DEFAULT)) {
        Assertions.assertEquals(ArrowMessage.HeaderType.SCHEMA, message.getMessageType());
        // Should have no body buffers
        Assertions.assertFalse(message.getBufs().iterator().hasNext());
        final Flight.FlightData protobufData = arrowMessageToProtobuf(marshaller, message)
            .toBuilder()
            .setDataBody(ByteString.EMPTY)
            .build();
        Assertions.assertEquals(0, protobufData.getDataBody().size());
        final ArrowMessage parsedMessage = marshaller.parse(new ByteArrayInputStream(protobufData.toByteArray()));
        // Should have no body buffers
        Assertions.assertFalse(parsedMessage.getBufs().iterator().hasNext());
        // Should not throw
        parsedMessage.asSchema();
      }
    }
  }

  @Test
  public void testGrpcInsecureLocation() throws Exception {
    Location location = Location.forGrpcInsecure(LOCALHOST, 9000);
    Assertions.assertEquals(
        new URI(LocationSchemes.GRPC_INSECURE, null, LOCALHOST, 9000, null, null, null),
        location.getUri());
    Assertions.assertEquals(new InetSocketAddress(LOCALHOST, 9000), location.toSocketAddress());
  }

  @Test
  public void testGrpcTlsLocation() throws Exception {
    Location location = Location.forGrpcTls(LOCALHOST, 9000);
    Assertions.assertEquals(
            new URI(LocationSchemes.GRPC_TLS, null, LOCALHOST, 9000, null, null, null),
            location.getUri());
    Assertions.assertEquals(new InetSocketAddress(LOCALHOST, 9000), location.toSocketAddress());
  }

  /**
   * An example FlightProducer for test purposes.
   */
  public static class Producer implements FlightProducer, AutoCloseable {
    static final byte[] TICKET_LARGE_BATCH = "large-batch".getBytes(StandardCharsets.UTF_8);

    private final BufferAllocator allocator;

    public Producer(BufferAllocator allocator) {
      super();
      this.allocator = allocator;
    }

    @Override
    public void listFlights(CallContext context, Criteria criteria,
        StreamListener<FlightInfo> listener) {
      if (criteria.getExpression().length > 0) {
        // Don't send anything if criteria are set
        listener.onCompleted();
      }

      Flight.FlightInfo getInfo = Flight.FlightInfo.newBuilder()
          .setFlightDescriptor(Flight.FlightDescriptor.newBuilder()
              .setType(DescriptorType.CMD)
              .setCmd(ByteString.copyFrom("cool thing", Charsets.UTF_8)))
          .build();
      try {
        listener.onNext(new FlightInfo(getInfo));
      } catch (URISyntaxException e) {
        listener.onError(e);
        return;
      }
      listener.onCompleted();
    }

    @Override
    public Runnable acceptPut(CallContext context, FlightStream flightStream, StreamListener<PutResult> ackStream) {
      return () -> {
        while (flightStream.next()) {
          // Drain the stream
        }
      };
    }

    @Override
    public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
      if (Arrays.equals(TICKET_LARGE_BATCH, ticket.getBytes())) {
        getLargeBatch(listener);
        return;
      }
      final int size = 10;

      IntVector iv = new IntVector("c1", allocator);
      VectorSchemaRoot root = VectorSchemaRoot.of(iv);
      listener.start(root);

      //batch 1
      root.allocateNew();
      for (int i = 0; i < size; i++) {
        iv.set(i, i);
      }
      iv.setValueCount(size);
      root.setRowCount(size);
      listener.putNext();

      // batch 2

      root.allocateNew();
      for (int i = 0; i < size; i++) {
        iv.set(i, i + size);
      }
      iv.setValueCount(size);
      root.setRowCount(size);
      listener.putNext();
      root.clear();
      listener.completed();
    }

    private void getLargeBatch(ServerStreamListener listener) {
      final List<FieldVector> vectors = new ArrayList<>();
      for (int col = 0; col < 128; col++) {
        final BigIntVector vector = new BigIntVector("f" + col, allocator);
        for (int row = 0; row < 65536; row++) {
          vector.setSafe(row, row);
        }
        vectors.add(vector);
      }
      try (final VectorSchemaRoot root = new VectorSchemaRoot(vectors)) {
        root.setRowCount(65536);
        listener.start(root);
        listener.putNext();
        listener.putNext();
        listener.completed();
      }
    }

    @Override
    public void close() throws Exception {
      allocator.close();
    }

    @Override
    public FlightInfo getFlightInfo(CallContext context,
        FlightDescriptor descriptor) {
      try {
        Flight.FlightInfo getInfo = Flight.FlightInfo.newBuilder()
            .setFlightDescriptor(Flight.FlightDescriptor.newBuilder()
                .setType(DescriptorType.CMD)
                .setCmd(ByteString.copyFrom("cool thing", Charsets.UTF_8)))
            .addEndpoint(
                Flight.FlightEndpoint.newBuilder().addLocation(new Location("https://example.com").toProtocol()))
            .build();
        return new FlightInfo(getInfo);
      } catch (URISyntaxException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void doAction(CallContext context, Action action,
        StreamListener<Result> listener) {
      switch (action.getType()) {
        case "hello": {
          listener.onNext(new Result("world".getBytes(Charsets.UTF_8)));
          listener.onCompleted();
          break;
        }
        case "hellooo": {
          listener.onNext(new Result("world".getBytes(Charsets.UTF_8)));
          listener.onNext(new Result("!".getBytes(Charsets.UTF_8)));
          listener.onCompleted();
          break;
        }
        default:
          listener.onError(CallStatus.UNIMPLEMENTED.withDescription("Action not implemented: " + action.getType())
              .toRuntimeException());
      }
    }

    @Override
    public void listActions(CallContext context,
        StreamListener<ActionType> listener) {
      listener.onNext(new ActionType("get", ""));
      listener.onNext(new ActionType("put", ""));
      listener.onNext(new ActionType("hello", ""));
      listener.onCompleted();
    }

  }


}
