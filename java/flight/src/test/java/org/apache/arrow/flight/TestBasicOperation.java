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

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.apache.arrow.flight.FlightClient.ClientStreamListener;
import org.apache.arrow.flight.impl.Flight;
import org.apache.arrow.flight.impl.Flight.FlightDescriptor.DescriptorType;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.protobuf.ByteString;

/**
 * Test the operations of a basic flight service.
 */
public class TestBasicOperation {

  /**
   * ARROW-6017: we should be able to construct locations for unknown schemes.
   */
  @Test
  public void unknownScheme() throws URISyntaxException {
    final Location location = new Location("s3://unknown");
    Assert.assertEquals("s3", location.getUri().getScheme());
  }

  @Test
  public void unknownSchemeRemote() throws Exception {
    test(c -> {
      try {
        final FlightInfo info = c.getInfo(FlightDescriptor.path("test"));
        Assert.assertEquals(new URI("https://example.com"), info.getEndpoints().get(0).getLocations().get(0).getUri());
      } catch (URISyntaxException e) {
        throw new RuntimeException(e);
      }
    });
  }

  @Test
  public void roundTripTicket() throws Exception {
    final Ticket ticket = new Ticket(new byte[]{0, 1, 2, 3, 4, 5});
    Assert.assertEquals(ticket, Ticket.deserialize(ticket.serialize()));
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
                Location.forGrpcInsecure("localhost", 50051))
        ), 200, 500);

    Assert.assertEquals(info1, FlightInfo.deserialize(info1.serialize()));
    Assert.assertEquals(info2, FlightInfo.deserialize(info2.serialize()));
    Assert.assertEquals(info3, FlightInfo.deserialize(info3.serialize()));
  }

  @Test
  public void roundTripDescriptor() throws Exception {
    final FlightDescriptor cmd = FlightDescriptor.command("test command".getBytes(StandardCharsets.UTF_8));
    Assert.assertEquals(cmd, FlightDescriptor.deserialize(cmd.serialize()));
    final FlightDescriptor path = FlightDescriptor.path("foo", "bar", "test.arrow");
    Assert.assertEquals(path, FlightDescriptor.deserialize(path.serialize()));
  }

  @Test
  public void getDescriptors() throws Exception {
    test(c -> {
      for (FlightInfo i : c.listFlights(Criteria.ALL)) {
        System.out.println(i.getDescriptor());
      }
    });
  }

  @Test
  public void getDescriptor() throws Exception {
    test(c -> {
      System.out.println(c.getInfo(FlightDescriptor.path("hello")).getDescriptor());
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

      Assert.assertTrue(stream.hasNext());
      Result r = stream.next();
      Assert.assertArrayEquals("world".getBytes(Charsets.UTF_8), r.getBody());
    });
    test(c -> {
      Iterator<Result> stream = c.doAction(new Action("hellooo"));

      Assert.assertTrue(stream.hasNext());
      Result r = stream.next();
      Assert.assertArrayEquals("world".getBytes(Charsets.UTF_8), r.getBody());

      Assert.assertTrue(stream.hasNext());
      r = stream.next();
      Assert.assertArrayEquals("!".getBytes(Charsets.UTF_8), r.getBody());
      Assert.assertFalse(stream.hasNext());
    });
  }

  @Test
  public void putStream() throws Exception {
    test((c, a) -> {
      final int size = 10;

      IntVector iv = new IntVector("c1", a);

      VectorSchemaRoot root = VectorSchemaRoot.of(iv);
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
    });
  }


  @Test
  public void getStream() throws Exception {
    test(c -> {
      FlightStream stream = c.getStream(new Ticket(new byte[0]));
      VectorSchemaRoot root = stream.getRoot();
      IntVector iv = (IntVector) root.getVector("c1");
      int value = 0;
      while (stream.next()) {
        for (int i = 0; i < root.getRowCount(); i++) {
          Assert.assertEquals(value, iv.get(i));
          value++;
        }
      }
    });
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
        FlightServer s =
            FlightTestUtil.getStartedServer(
                (location) -> FlightServer.builder(a, location, producer).build()
            )) {

      try (
          FlightClient c = FlightClient.builder(a, s.getLocation()).build()
      ) {
        try (BufferAllocator testAllocator = a.newChildAllocator("testcase", 0, Long.MAX_VALUE)) {
          consumer.accept(c, testAllocator);
        }
      }
    }
  }

  /**
   * An example FlightProducer for test purposes.
   */
  public static class Producer implements FlightProducer, AutoCloseable {

    private final BufferAllocator allocator;

    public Producer(BufferAllocator allocator) {
      super();
      this.allocator = allocator;
    }

    @Override
    public void listFlights(CallContext context, Criteria criteria,
        StreamListener<FlightInfo> listener) {
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
        try (VectorSchemaRoot root = flightStream.getRoot()) {
          while (flightStream.next()) {

          }
        }
      };
    }

    @Override
    public void getStream(CallContext context, Ticket ticket,
        ServerStreamListener listener) {
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
          listener.onError(new UnsupportedOperationException());
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
