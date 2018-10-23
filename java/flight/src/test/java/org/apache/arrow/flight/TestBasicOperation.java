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

import java.util.concurrent.Callable;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.apache.arrow.flight.FlightClient.ClientStreamListener;
import org.apache.arrow.flight.auth.ServerAuthHandler;
import org.apache.arrow.flight.impl.Flight;
import org.apache.arrow.flight.impl.Flight.FlightDescriptor.DescriptorType;
import org.apache.arrow.flight.impl.Flight.FlightGetInfo;
import org.apache.arrow.flight.impl.Flight.PutResult;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.protobuf.ByteString;

/**
 * Test the operations of a basic flight service.
 */
public class TestBasicOperation {

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
      Result r = c.doAction(new Action("hello")).next();
      System.out.println(new String(r.getBody(), Charsets.UTF_8));
    });
  }

  @Test
  public void putStream() throws Exception {
    test((c, a) -> {
      final int size = 10;

      IntVector iv = new IntVector("c1", a);

      VectorSchemaRoot root = VectorSchemaRoot.of(iv);
      ClientStreamListener listener = c.startPut(FlightDescriptor.path("hello"), root);

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
        FlightServer s = new FlightServer(a, 12233, producer, ServerAuthHandler.NO_OP);) {

      s.start();

      try (
          FlightClient c = new FlightClient(a, new Location("localhost", 12233));
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
  public class Producer implements FlightProducer, AutoCloseable {

    private final BufferAllocator allocator;

    public Producer(BufferAllocator allocator) {
      super();
      this.allocator = allocator;
    }

    @Override
    public void listFlights(Criteria criteria, StreamListener<FlightInfo> listener) {
      FlightGetInfo getInfo = FlightGetInfo.newBuilder()
          .setFlightDescriptor(Flight.FlightDescriptor.newBuilder()
              .setType(DescriptorType.CMD)
              .setCmd(ByteString.copyFrom("cool thing", Charsets.UTF_8)))
          .build();
      listener.onNext(new FlightInfo(getInfo));
      listener.onCompleted();
    }

    @Override
    public Callable<PutResult> acceptPut(FlightStream flightStream) {
      return () -> {
        try (VectorSchemaRoot root = flightStream.getRoot()) {
          while (flightStream.next()) {

          }
          return PutResult.getDefaultInstance();
        }
      };
    }

    @Override
    public void getStream(Ticket ticket, ServerStreamListener listener) {
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
    public FlightInfo getFlightInfo(FlightDescriptor descriptor) {
      FlightGetInfo getInfo = FlightGetInfo.newBuilder()
          .setFlightDescriptor(Flight.FlightDescriptor.newBuilder()
              .setType(DescriptorType.CMD)
              .setCmd(ByteString.copyFrom("cool thing", Charsets.UTF_8)))
          .build();
      return new FlightInfo(getInfo);
    }

    @Override
    public Result doAction(Action action) {
      switch (action.getType()) {
        case "hello":
          return new Result("world".getBytes(Charsets.UTF_8));
        default:
          throw new UnsupportedOperationException();
      }
    }

    @Override
    public void listActions(StreamListener<ActionType> listener) {
      listener.onNext(new ActionType("get", ""));
      listener.onNext(new ActionType("put", ""));
      listener.onNext(new ActionType("hello", ""));
      listener.onCompleted();
    }

  }


}
